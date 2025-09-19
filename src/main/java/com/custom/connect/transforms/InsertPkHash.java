package com.custom.connect.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Custom SMT that adds a primary key hash column to flattened records.
 * 
 * This SMT is designed to work with flattened records,
 * when the record value contains the actual data fields (not wrapped in change event structure).
 * 
 * Features:
 * - Automatically detects primary key fields from the record key
 * - If PK exists: hashes only the primary key fields
 * - If no PK: generates a random UUID for unique identification
 * - Adds a configurable hash column (default: "pk_hash")
 * - Supports multiple hash algorithms (SHA-256, SHA-1, MD5, SHA-512)
 * - Configurable null handling
 * - Robust error handling with configurable failure policies
 * 
 * Configuration:
 * - pk.hash.column.name: Name of the hash column to add (default: "pk_hash")
 * - hash.algorithm: Hash algorithm to use (default: "SHA-256")
 * - include.nulls: Whether to include null values in hash calculation (default: true)
 * - failure.handling: What to do on errors - "CONTINUE" or "STOP" (default: "CONTINUE")
 */
public class InsertPkHash<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(InsertPkHash.class);

    // Configuration constants
    public static final String PK_HASH_COLUMN_NAME_CONFIG = "pk.hash.column.name";
    public static final String HASH_ALGORITHM_CONFIG = "hash.algorithm";
    public static final String INCLUDE_NULLS_CONFIG = "include.nulls";
    public static final String FAILURE_HANDLING_CONFIG = "failure.handling";
    
    // Instance variables
    private String pkHashColumnName;
    private String hashAlgorithm;
    private boolean includeNulls;
    private String failureHandling;

    @Override
    public void configure(Map<String, ?> configs) {
        try {
            // Configure hash-specific settings with safe defaults
            this.pkHashColumnName = getStringConfig(configs, PK_HASH_COLUMN_NAME_CONFIG, "pk_hash");
            this.hashAlgorithm = getStringConfig(configs, HASH_ALGORITHM_CONFIG, "SHA-256");
            this.includeNulls = getBooleanConfig(configs, INCLUDE_NULLS_CONFIG, true);
            this.failureHandling = getStringConfig(configs, FAILURE_HANDLING_CONFIG, "CONTINUE");
            
            log.info("InsertPkHash configured: pkHashColumnName={}, hashAlgorithm={}, includeNulls={}, failureHandling={}", 
                    pkHashColumnName, hashAlgorithm, includeNulls, failureHandling);
                    
        } catch (Exception e) {
            log.error("Failed to configure InsertPkHash", e);
            throw new RuntimeException("Configuration failed", e);
        }
    }

    @Override
    public R apply(R record) {
        try {
            log.debug("Processing record from topic: {}, partition: {}", record.topic(), record.kafkaPartition());
            
            // If the record value is null, return as-is
            if (record.value() == null) {
                log.debug("Record value is null, returning as-is");
                return record;
            }
            
            // If the value is not a Struct, return as-is
            if (!(record.value() instanceof Struct)) {
                log.info("Record value is not a Struct (type: {}), passing through unchanged", 
                        record.value().getClass().getSimpleName());
                return record;
            }
            
            // Add hash to the record
            return addHashToRecord(record);
            
        } catch (Exception e) {
            log.error("Failed to process record from topic: {}, partition: {}, error: {}", 
                    record.topic(), record.kafkaPartition(), e.getMessage(), e);
            
            if ("STOP".equals(failureHandling)) {
                throw new RuntimeException("Record processing failed and failure handling is set to STOP", e);
            } else {
                log.warn("Continuing with original record due to failure handling policy: {}", failureHandling);
                return record;
            }
        }
    }

    private R addHashToRecord(R record) {
        try {
            log.debug("addHashToRecord called for record: {}", record);
            Struct value = (Struct) record.value();
            
            // Extract primary key fields from the record key
            Map<String, Object> pkFields = extractPrimaryKeyFieldsFromKey(record, value);
            log.debug("Extracted PK fields: {}", pkFields);
            
            // Calculate hash
            String hash = calculatePKHash(value, pkFields, record.topic(), record.kafkaPartition());
            log.debug("Calculated hash: {}", hash);
            
            // Create new schema with hash field
            Schema newValueSchema = addPKHashFieldToSchema(value.schema());
            log.debug("Created new schema with {} fields", newValueSchema.fields().size());
            
            // Create new struct with hash field
            Struct newValue = addPKHashToStruct(value, newValueSchema, hash);
            log.debug("Created new struct with hash field");
            
            // Create new record
            R newRecord = record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    newValueSchema,
                    newValue,
                    record.timestamp(),
                    record.headers()
            );
            
            log.debug("Created new record with hash field");
            return newRecord;
            
        } catch (Exception e) {
            log.error("Failed to add hash to record: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to add hash to record", e);
        }
    }

    /**
     * Extract primary key fields from the record key.
     * If the key is a Struct, extract all its fields as primary keys.
     * If the key is not a Struct or is null, return empty map (will hash all fields).
     */
    private Map<String, Object> extractPrimaryKeyFieldsFromKey(R record, Struct value) {
        Map<String, Object> pkFields = new HashMap<>();
        
        try {
            if (record.key() instanceof Struct) {
                Struct keyStruct = (Struct) record.key();
                log.debug("Key is a Struct with {} fields", keyStruct.schema().fields().size());
                
                for (Field keyField : keyStruct.schema().fields()) {
                    String fieldName = keyField.name();
                    Object fieldValue = keyStruct.get(keyField);
                    
                    // Check if this field exists in the value struct
                    if (value.schema().field(fieldName) != null) {
                        pkFields.put(fieldName, fieldValue);
                        log.debug("Added PK field: {} = {}", fieldName, fieldValue);
                    } else {
                        log.debug("PK field {} not found in value struct, skipping", fieldName);
                    }
                }
            } else {
                log.debug("Key is not a Struct (type: {}), will hash all fields", 
                        record.key() != null ? record.key().getClass().getSimpleName() : "null");
            }
        } catch (Exception e) {
            log.warn("Error extracting primary key fields: {}", e.getMessage());
        }
        
        return pkFields;
    }

    /**
     * Add the hash field to the schema.
     */
    private Schema addPKHashFieldToSchema(Schema originalSchema) {
        SchemaBuilder builder = SchemaBuilder.struct()
                .name(originalSchema.name())
                .doc(originalSchema.doc());
        
        // Copy all original fields
        for (Field field : originalSchema.fields()) {
            builder.field(field.name(), field.schema());
        }
        
        // Add the hash field
        builder.field(pkHashColumnName, Schema.STRING_SCHEMA);
        
        return builder.build();
    }

    /**
     * Add the hash value to the struct.
     */
    private Struct addPKHashToStruct(Struct originalStruct, Schema newSchema, String hash) {
        Struct newStruct = new Struct(newSchema);
        
        // Copy all original field values
        for (Field field : originalStruct.schema().fields()) {
            newStruct.put(field.name(), originalStruct.get(field));
        }
        
        // Add the hash value
        newStruct.put(pkHashColumnName, hash);
        
        return newStruct;
    }

    /**
     * Calculate hash based on primary key fields or generate UUID.
     * If PK fields exist: hash only PK fields
     * If no PK fields: generate a random UUID for unique identification
     */
    private String calculatePKHash(Struct struct, Map<String, Object> pkFields, String topic, Integer partition) {
        try {
            if (!pkFields.isEmpty()) {
                // If primary key exists: hash ONLY the primary key fields
                log.debug("Primary key found, hashing only PK fields: {}", pkFields.keySet());
                
                MessageDigest digest = MessageDigest.getInstance(hashAlgorithm);
                StringBuilder sb = new StringBuilder();

                // Sort fields by name for consistent hashing
                Map<String, Object> fieldMap = new HashMap<>();
                for (String pkFieldName : pkFields.keySet()) {
                    Field field = struct.schema().field(pkFieldName);
                    if (field != null) {
                        Object value = struct.get(field);
                        fieldMap.put(field.name(), value);
                    }
                }

                fieldMap.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .forEach(entry -> {
                            String fieldName = entry.getKey();
                            Object fieldValue = entry.getValue();
                            
                            sb.append(fieldName).append("=");
                            
                            if (fieldValue == null) {
                                if (includeNulls) {
                                    sb.append("null");
                                }
                            } else {
                                sb.append(fieldValue.toString());
                            }
                            sb.append("|");
                        });

                String dataToHash = sb.toString();
                log.debug("Data to hash for topic {}: {}, partition {}: {} (length: {})", topic, dataToHash, partition, dataToHash.length());
                
                byte[] hashBytes = digest.digest(dataToHash.getBytes(StandardCharsets.UTF_8));
                
                StringBuilder hexString = new StringBuilder();
                for (byte b : hashBytes) {
                    String hex = Integer.toHexString(0xff & b);
                    if (hex.length() == 1) {
                        hexString.append('0');
                    }
                    hexString.append(hex);
                }
                
                String hash = hexString.toString();
                log.debug("Generated hash for topic {}: {}, partition {}: {} (algorithm: {})", topic, hash, partition, hashAlgorithm);
                return hash;
                
            } else {
                // If no primary key: generate a random UUID for unique identification
                log.debug("No primary key found, generating random UUID");
                String uuid = UUID.randomUUID().toString();
                log.debug("Generated UUID for topic {}: {}, partition {}: {}", topic, uuid, partition);
                return uuid;
            }
            
        } catch (NoSuchAlgorithmException e) {
            log.error("Hash algorithm {} not supported for topic {}, partition {}: {}", hashAlgorithm, topic, partition, e.getMessage(), e);
            return "HASH_ERROR_ALGORITHM_NOT_SUPPORTED";
        } catch (Exception e) {
            log.error("Hash calculation failed for topic {}, partition {}: {}", topic, partition, e.getMessage(), e);
            return "HASH_ERROR_CALCULATION_FAILED";
        }
    }

    @Override
    public void close() {
        log.debug("InsertPkHash closed");
    }

    @Override
    public org.apache.kafka.common.config.ConfigDef config() {
        return new org.apache.kafka.common.config.ConfigDef()
                .define(PK_HASH_COLUMN_NAME_CONFIG, 
                        org.apache.kafka.common.config.ConfigDef.Type.STRING, 
                        "pk_hash", 
                        org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM, 
                        "Name of the hash column to add")
                .define(HASH_ALGORITHM_CONFIG, 
                        org.apache.kafka.common.config.ConfigDef.Type.STRING, 
                        "SHA-256", 
                        org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM, 
                        "Hash algorithm to use (SHA-256, SHA-1, MD5, SHA-512)")
                .define(INCLUDE_NULLS_CONFIG, 
                        org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN, 
                        true, 
                        org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM, 
                        "Whether to include null values in hash calculation")
                .define(FAILURE_HANDLING_CONFIG, 
                        org.apache.kafka.common.config.ConfigDef.Type.STRING, 
                        "CONTINUE", 
                        org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM, 
                        "What to do on errors: CONTINUE or STOP");
    }

    // Helper methods for configuration
    private String getStringConfig(Map<String, ?> configs, String key, String defaultValue) {
        Object value = configs.get(key);
        return value != null ? value.toString() : defaultValue;
    }

    private boolean getBooleanConfig(Map<String, ?> configs, String key, boolean defaultValue) {
        Object value = configs.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        return defaultValue;
    }
}
