package com.github.bsvicente.cloudevents;

import com.github.bsvicente.cloudevents.processor.CloudEventsConverter;
import io.cloudevents.CloudEvent;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

public class CloudEventsConnectParserTransforms<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String FIELD_KEY_CONFIG = "key";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_KEY_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Transforms Plain Json to Connect Struct");

    private static final Logger LOG = LoggerFactory.getLogger(CloudEventsConnectParserTransforms.class);

    @Override
    public R apply(R r) {

        var type = r.value().getClass().getName();

        LOG.debug("Converting record from " + type);

        if (r.value() instanceof String) {
            Objects.requireNonNull(r.value());

            LOG.debug("Received text: {}", r.value());

            // Setting key schema as Optional
            Schema keySchema = Schema.OPTIONAL_BYTES_SCHEMA;

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
                    Values.inferSchema(message), message, r.timestamp(), r.headers());

        } else {

            LOG.debug("Unexpected message type: {}", r.value().getClass().getCanonicalName());

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
