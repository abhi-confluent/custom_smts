# Custom SMT for Kafka Connect

This project contains custom Single Message Transforms (SMTs) for Kafka Connect that provide enhanced functionality for data processing and routing.

## Available SMTs

### 1. InsertPkHash

**Purpose**: Adds a `pk_hash` column to records based on primary key detection and hashing logic.

**Features**:
- Automatically detects primary keys from the Kafka Connect record key
- Hashes primary key fields if present
- Generates a random UUID if no primary key is found
- Works with Confluent Cloud's managed `ExtractNewRecordState` SMT

**Configuration**:
```json
{
  "transforms": "insertPkHash",
  "transforms.insertPkHash.type": "com.custom.connect.transforms.InsertPkHash",
  "transforms.insertPkHash.pk.hash.algorithm": "SHA-256",
  "transforms.insertPkHash.pk.hash.column.name": "pk_hash"
}
```

**Configuration Parameters**:
- `pk.hash.algorithm`: Hash algorithm to use (default: SHA-256)
- `pk.hash.column.name`: Name of the hash column to add (default: pk_hash)

**Usage Example**:
```json
{
  "name": "debezium-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "localhost",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "postgres",
    "database.server.name": "postgres",
    "table.include.list": "public.customers",
    "transforms": "unwrap,insertPkHash",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "drop",
    "transforms.unwrap.add.fields": "op,ts_ms,source.ts_ms,source.lsn,source.txId,source.xmin",
    "transforms.insertPkHash.type": "com.custom.connect.transforms.InsertPkHash",
    "transforms.insertPkHash.pk.hash.algorithm": "SHA-256",
    "transforms.insertPkHash.pk.hash.column.name": "pk_hash"
  }
}
```

### 2. RegexRouter

**Purpose**: Routes records to different topics based on regex patterns.

**Features**:
- Pattern-based topic routing
- Flexible regex matching
- Configurable topic naming

**Configuration**:
```json
{
  "transforms": "regexRouter",
  "transforms.regexRouter.type": "com.custom.connect.transforms.RegexRouter",
  "transforms.regexRouter.regex": ".*",
  "transforms.regexRouter.replacement": "routed_topic"
}
```

**Configuration Parameters**:
- `regex`: Regex pattern to match (default: .*)
- `replacement`: Topic name replacement (default: routed_topic)

**Usage Example**:
```json
{
  "name": "regex-router-connector",
  "config": {
    "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
    "file": "/tmp/test.txt",
    "topic": "test-topic",
    "transforms": "regexRouter",
    "transforms.regexRouter.type": "com.custom.connect.transforms.RegexRouter",
    "transforms.regexRouter.regex": "test-.*",
    "transforms.regexRouter.replacement": "routed_test_topic"
  }
}
```

## Building the Project

### Prerequisites
- Java 8 or higher
- Maven 3.6 or higher

### Build Commands
```bash
# Clean and compile
mvn clean compile

# Run tests
mvn test

# Build JAR
mvn clean package

# Build uber JAR with all dependencies
mvn clean package
```

The build will create:
- `target/custom-smt-regex-router-1.0.0.jar` - Standard JAR
- `target/custom-smt-regex-router-1.0.0-all.jar` - Uber JAR with all dependencies

## Installation

1. Build the project using Maven
2. Upload the `custom-smt-regex-router-1.0.0-all.jar` to your Kafka Connect cluster
3. Restart Kafka Connect to load the new SMTs
4. Configure your connectors to use the SMTs

## Confluent Cloud Compatibility

These SMTs are designed to work with Confluent Cloud's managed environment:

- **InsertPkHash**: Works with Confluent Cloud's managed `ExtractNewRecordState` SMT
- **RegexRouter**: Compatible with Confluent Cloud's managed `TopicRegexRouter` SMT

## Dependencies

- Apache Kafka Connect API 3.4.0
- Apache Kafka Connect Transforms 3.4.0
- SLF4J API 1.7.36
- Debezium Core 2.3.4.Final (for Confluent Cloud compatibility)
- Debezium API 2.3.4.Final (for Confluent Cloud compatibility)

## Testing

Run the test suite:
```bash
mvn test
```

Individual test classes:
- `InsertPkHashTest` - Tests for InsertPkHash SMT
- `RegexRouterTest` - Tests for RegexRouter SMT

## License

This project is licensed under the Apache License 2.0.


