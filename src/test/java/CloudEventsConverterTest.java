import com.github.bsvicente.cloudevents.processor.CloudEventsConverter;
import io.cloudevents.CloudEvent;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Objects;

public class CloudEventsConverterTest {

    @Test
    public void getJsonToCloudEventsRecordBuilder() throws Exception {

        File file = new File("src/test/resources/sampleCE.json");

        CloudEvent cloudEvent = CloudEventsConverter.createCloudEventsFromJson(Files.readString(file.toPath()));

        assert (!Objects.isNull(cloudEvent));
    }
    @Test
    public void getJsonToCloudEventsKafkaRecordBuilder() throws Exception {

        File file = new File("src/test/resources/sampleNullCE.json");

        // RecordHeaders and ConnectHeaders are differents...
        // Create RecordHeaders
        RecordHeaders recordHeaders = new RecordHeaders();

        // Transform to CloudEvents
        CloudEvent cloudEvent = CloudEventsConverter.createCloudEventsFromJson(Files.readString(file.toPath()));

        // Generate new json and populate RecordHeaders
        String message = CloudEventsConverter.createKafkaMessage(recordHeaders, cloudEvent, "test");

        assert (!Objects.isNull(message));
    }
}