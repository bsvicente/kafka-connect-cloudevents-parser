import com.github.bsvicente.cloudevents.processor.CloudEventsConverter;
import io.cloudevents.CloudEvent;
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
}