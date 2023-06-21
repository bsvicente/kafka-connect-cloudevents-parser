package com.github.bsvicente.cloudevents;

import com.github.bsvicente.cloudevents.processor.CloudEventsConverter;
import io.cloudevents.CloudEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.transforms.Transformation;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class CloudEventsConnectParserTransforms<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String FIELD_KEY_CONFIG = "key";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_KEY_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Transforms Plain Json to Connect Struct");

    @Override
    public R apply(R r) {

        var type = r.value().getClass().getName();

        log.debug("Converting record from " + type);

        if (r.value() instanceof String) {

            Objects.requireNonNull(r.value());

            log.debug("Received text: {}", r.value());

            // Setting key schema as Optional
            Schema keySchema = Schema.OPTIONAL_STRING_SCHEMA;

            // RecordHeaders and ConnectHeaders are differents...
            // Create RecordHeaders
            RecordHeaders recordHeaders = new RecordHeaders();

            // Transform to CloudEvents
            CloudEvent cloudEvent = CloudEventsConverter.createCloudEventsFromJson((String) r.value());

            // Generate new json and populate RecordHeaders
            String message = CloudEventsConverter.createKafkaMessage(recordHeaders, cloudEvent, r.topic());

            // Convert to ConnectHeaders
            recordHeaders.forEach(
                    header -> r.headers().addString(header.key(), new String(header.value(), StandardCharsets.UTF_8))
            );

            // Setting Subject as Key
            SchemaAndValue key = new SchemaAndValue(keySchema, cloudEvent.getSubject());

            return r.newRecord(r.topic(), r.kafkaPartition(), key.schema(), key.value(),
                    Schema.OPTIONAL_STRING_SCHEMA, message, r.timestamp(), r.headers());

        } else {

            log.info("Unexpected message type: {}", r.value().getClass().getCanonicalName());

            return r;
        }
    }


    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
