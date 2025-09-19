package com.custom.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class InsertPkHashTest {

    @Test
    public void testInsertPkHashWithPrimaryKey() {
        // Create schema for flattened record (after ExtractNewRecordState)
        Schema keySchema = SchemaBuilder.struct()
                .field("customer_id", Schema.INT32_SCHEMA)
                .build();

        Schema valueSchema = SchemaBuilder.struct()
                .field("customer_id", Schema.INT32_SCHEMA)
                .field("first_name", Schema.STRING_SCHEMA)
                .field("last_name", Schema.STRING_SCHEMA)
                .field("email", Schema.STRING_SCHEMA)
                .build();

        // Create the key
        Struct keyValue = new Struct(keySchema);
        keyValue.put("customer_id", 123);

        // Create the flattened value (after ExtractNewRecordState)
        Struct recordValue = new Struct(valueSchema);
        recordValue.put("customer_id", 123);
        recordValue.put("first_name", "John");
        recordValue.put("last_name", "Doe");
        recordValue.put("email", "john.doe@example.com");

        SourceRecord record = new SourceRecord(
                Map.of("server", "postgres"),
                Map.of("lsid", 12345L),
                "customers",
                keySchema, keyValue,
                valueSchema, recordValue
        );

        // Test InsertPkHash
        InsertPkHash<SourceRecord> insertPkHash = new InsertPkHash<>();
        Map<String, Object> config = new HashMap<>();
        config.put("pk.hash.column.name", "pk_hash");
        config.put("hash.algorithm", "SHA-256");
        config.put("include.nulls", true);
        config.put("failure.handling", "CONTINUE");
        insertPkHash.configure(config);

        SourceRecord result = insertPkHash.apply(record);

        // Verify the result
        assertNotNull("Result should not be null", result);
        assertTrue("Result value should be a Struct", result.value() instanceof Struct);
        
        Struct resultValue = (Struct) result.value();
        assertTrue("Should have pk_hash field", resultValue.schema().field("pk_hash") != null);
        assertNotNull("pk_hash value should not be null", resultValue.get("pk_hash"));
        
        // Verify original fields are preserved
        assertEquals("customer_id should be preserved", 123, resultValue.get("customer_id"));
        assertEquals("first_name should be preserved", "John", resultValue.get("first_name"));
        assertEquals("last_name should be preserved", "Doe", resultValue.get("last_name"));
        assertEquals("email should be preserved", "john.doe@example.com", resultValue.get("email"));
        
        // Verify schema has one additional field
        assertEquals("Schema should have 5 fields (4 original + 1 hash)", 5, resultValue.schema().fields().size());
    }

    @Test
    public void testInsertPkHashWithoutPrimaryKey() {
        // Create schema for flattened record without primary key in key
        Schema keySchema = SchemaBuilder.struct()
                .field("some_key", Schema.STRING_SCHEMA)
                .build();

        Schema valueSchema = SchemaBuilder.struct()
                .field("customer_id", Schema.INT32_SCHEMA)
                .field("first_name", Schema.STRING_SCHEMA)
                .field("last_name", Schema.STRING_SCHEMA)
                .field("email", Schema.STRING_SCHEMA)
                .build();

        // Create the key (different from value fields)
        Struct keyValue = new Struct(keySchema);
        keyValue.put("some_key", "key123");

        // Create the flattened value
        Struct recordValue = new Struct(valueSchema);
        recordValue.put("customer_id", 123);
        recordValue.put("first_name", "John");
        recordValue.put("last_name", "Doe");
        recordValue.put("email", "john.doe@example.com");

        SourceRecord record = new SourceRecord(
                Map.of("server", "postgres"),
                Map.of("lsid", 12345L),
                "customers",
                keySchema, keyValue,
                valueSchema, recordValue
        );

        // Test InsertPkHash
        InsertPkHash<SourceRecord> insertPkHash = new InsertPkHash<>();
        Map<String, Object> config = new HashMap<>();
        config.put("pk.hash.column.name", "pk_hash");
        config.put("hash.algorithm", "SHA-256");
        config.put("include.nulls", true);
        config.put("failure.handling", "CONTINUE");
        insertPkHash.configure(config);

        SourceRecord result = insertPkHash.apply(record);

        // Verify the result
        assertNotNull("Result should not be null", result);
        assertTrue("Result value should be a Struct", result.value() instanceof Struct);
        
        Struct resultValue = (Struct) result.value();
        assertTrue("Should have pk_hash field", resultValue.schema().field("pk_hash") != null);
        assertNotNull("pk_hash value should not be null", resultValue.get("pk_hash"));
        
        // Verify original fields are preserved
        assertEquals("customer_id should be preserved", 123, resultValue.get("customer_id"));
        assertEquals("first_name should be preserved", "John", resultValue.get("first_name"));
        assertEquals("last_name should be preserved", "Doe", resultValue.get("last_name"));
        assertEquals("email should be preserved", "john.doe@example.com", resultValue.get("email"));
        
        // Verify schema has one additional field
        assertEquals("Schema should have 5 fields (4 original + 1 hash)", 5, resultValue.schema().fields().size());
        
        // Verify that pk_hash is a UUID (36 characters with dashes)
        String pkHash = resultValue.get("pk_hash").toString();
        assertTrue("pk_hash should be a UUID format", pkHash.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"));
    }

    @Test
    public void testInsertPkHashWithNullValues() {
        // Create schema with optional fields
        Schema keySchema = SchemaBuilder.struct()
                .field("customer_id", Schema.INT32_SCHEMA)
                .build();

        Schema valueSchema = SchemaBuilder.struct()
                .field("customer_id", Schema.INT32_SCHEMA)
                .field("first_name", Schema.STRING_SCHEMA)
                .field("last_name", SchemaBuilder.string().optional().build())
                .field("email", Schema.STRING_SCHEMA)
                .build();

        // Create the key
        Struct keyValue = new Struct(keySchema);
        keyValue.put("customer_id", 123);

        // Create the flattened value with null
        Struct recordValue = new Struct(valueSchema);
        recordValue.put("customer_id", 123);
        recordValue.put("first_name", "John");
        recordValue.put("last_name", null); // null value
        recordValue.put("email", "john.doe@example.com");

        SourceRecord record = new SourceRecord(
                Map.of("server", "postgres"),
                Map.of("lsid", 12345L),
                "customers",
                keySchema, keyValue,
                valueSchema, recordValue
        );

        // Test InsertPkHash
        InsertPkHash<SourceRecord> insertPkHash = new InsertPkHash<>();
        Map<String, Object> config = new HashMap<>();
        config.put("pk.hash.column.name", "pk_hash");
        config.put("hash.algorithm", "SHA-256");
        config.put("include.nulls", true);
        config.put("failure.handling", "CONTINUE");
        insertPkHash.configure(config);

        SourceRecord result = insertPkHash.apply(record);

        // Verify the result
        assertNotNull("Result should not be null", result);
        assertTrue("Result value should be a Struct", result.value() instanceof Struct);
        
        Struct resultValue = (Struct) result.value();
        assertTrue("Should have pk_hash field", resultValue.schema().field("pk_hash") != null);
        assertNotNull("pk_hash value should not be null", resultValue.get("pk_hash"));
        
        // Verify null handling
        assertNull("last_name should remain null", resultValue.get("last_name"));
    }

    @Test
    public void testInsertPkHashWithDifferentHashAlgorithm() {
        // Create schema
        Schema keySchema = SchemaBuilder.struct()
                .field("customer_id", Schema.INT32_SCHEMA)
                .build();

        Schema valueSchema = SchemaBuilder.struct()
                .field("customer_id", Schema.INT32_SCHEMA)
                .field("first_name", Schema.STRING_SCHEMA)
                .build();

        // Create the key
        Struct keyValue = new Struct(keySchema);
        keyValue.put("customer_id", 123);

        // Create the flattened value
        Struct recordValue = new Struct(valueSchema);
        recordValue.put("customer_id", 123);
        recordValue.put("first_name", "John");

        SourceRecord record = new SourceRecord(
                Map.of("server", "postgres"),
                Map.of("lsid", 12345L),
                "customers",
                keySchema, keyValue,
                valueSchema, recordValue
        );

        // Test InsertPkHash with MD5
        InsertPkHash<SourceRecord> insertPkHash = new InsertPkHash<>();
        Map<String, Object> config = new HashMap<>();
        config.put("pk.hash.column.name", "pk_hash");
        config.put("hash.algorithm", "MD5");
        config.put("include.nulls", true);
        config.put("failure.handling", "CONTINUE");
        insertPkHash.configure(config);

        SourceRecord result = insertPkHash.apply(record);

        // Verify the result
        assertNotNull("Result should not be null", result);
        assertTrue("Result value should be a Struct", result.value() instanceof Struct);
        
        Struct resultValue = (Struct) result.value();
        assertTrue("Should have pk_hash field", resultValue.schema().field("pk_hash") != null);
        assertNotNull("pk_hash value should not be null", resultValue.get("pk_hash"));
        
        // MD5 hash should be 32 characters
        String hash = resultValue.get("pk_hash").toString();
        assertEquals("MD5 hash should be 32 characters", 32, hash.length());
    }

    @Test
    public void testInsertPkHashWithNullRecord() {
        // Test with null record value
        SourceRecord record = new SourceRecord(
                Map.of("server", "postgres"),
                Map.of("lsid", 12345L),
                "customers",
                null, null,
                null, null
        );

        InsertPkHash<SourceRecord> insertPkHash = new InsertPkHash<>();
        Map<String, Object> config = new HashMap<>();
        config.put("pk.hash.column.name", "pk_hash");
        config.put("hash.algorithm", "SHA-256");
        config.put("include.nulls", true);
        config.put("failure.handling", "CONTINUE");
        insertPkHash.configure(config);

        SourceRecord result = insertPkHash.apply(record);

        // Should return the original record unchanged
        assertNotNull("Result should not be null", result);
        assertNull("Result value should be null", result.value());
    }

    @Test
    public void testInsertPkHashWithNonStructValue() {
        // Test with non-Struct value
        SourceRecord record = new SourceRecord(
                Map.of("server", "postgres"),
                Map.of("lsid", 12345L),
                "customers",
                null, null,
                Schema.STRING_SCHEMA, "some string value"
        );

        InsertPkHash<SourceRecord> insertPkHash = new InsertPkHash<>();
        Map<String, Object> config = new HashMap<>();
        config.put("pk.hash.column.name", "pk_hash");
        config.put("hash.algorithm", "SHA-256");
        config.put("include.nulls", true);
        config.put("failure.handling", "CONTINUE");
        insertPkHash.configure(config);

        SourceRecord result = insertPkHash.apply(record);

        // Should return the original record unchanged
        assertNotNull("Result should not be null", result);
        assertEquals("Result value should be unchanged", "some string value", result.value());
    }
}
