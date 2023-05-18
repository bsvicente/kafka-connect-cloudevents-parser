package com.github.bsvicente.cloudevents.processor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.Encoding;
import io.cloudevents.jackson.JsonFormat;
import io.cloudevents.kafka.CloudEventSerializer;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CloudEventsConverter {

    private static final CloudEventSerializer eventFormatSerializer = new CloudEventSerializer();
    private static final Logger log = LoggerFactory.getLogger(CloudEventSerializer.class);

    public static CloudEvent createCloudEventsFromJson(String plainJson) {

        return new JsonFormat().deserialize(plainJson.getBytes(StandardCharsets.UTF_8));
    }

    public static String createKafkaMessage(Headers headers, CloudEvent cloudEvent, String topic) {
        return createKafkaMessage(headers, cloudEvent, topic, Encoding.BINARY);
    }

    public static String createKafkaMessage(Headers headers, CloudEvent cloudEvent, String topic, Encoding encoding) {
        eventFormatSerializer.configure(Map.of(CloudEventSerializer.EVENT_FORMAT_CONFIG, encoding), false);

        byte[] serialized = eventFormatSerializer.serialize(topic, headers, cloudEvent);

        return new String(serialized);

    }
}
