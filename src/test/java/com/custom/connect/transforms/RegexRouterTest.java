package com.custom.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class RegexRouterTest {

    private RegexRouter<SinkRecord> regexRouter;

    @Before
    public void setup() {
        regexRouter = new RegexRouter<>();
    }

    @Test
    public void testTopicRouting() {
        Map<String, Object> config = new HashMap<>();
        config.put("regex", "([^.]+)\\..*\\.(.+)");
        config.put("replacement", "$1.$2");
        regexRouter.configure(config);

        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();

        Struct value = new Struct(schema);
        value.put("id", 1);
        value.put("name", "John");

        SinkRecord record = new SinkRecord(
                "coyote.public.accounts", 0, Schema.STRING_SCHEMA, "key",
                schema, value, 12345L
        );

        SinkRecord transformedRecord = regexRouter.apply(record);

        assertNotNull("Transformed record should not be null", transformedRecord);
        assertEquals("Topic should be routed correctly", "coyote.accounts", transformedRecord.topic());
        assertEquals("Value should be preserved", value, transformedRecord.value());
    }
}


