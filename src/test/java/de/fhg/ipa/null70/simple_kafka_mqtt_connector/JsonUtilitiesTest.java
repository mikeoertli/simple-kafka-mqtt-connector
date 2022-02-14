package de.fhg.ipa.null70.simple_kafka_mqtt_connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * Unit tests for basic functionality offered by {@link JsonUtilities}
 *
 * @since 0.1.0
 */
class JsonUtilitiesTest
{

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    void getFormattedJsonString() throws JsonProcessingException
    {
        String unformatted = "{\"deviceId\":\"temp1\",\"poweredOn\":true,\"batteryPercent\":92.62947,\"timestamp\":\"2022-02-14T11:13:55.291-0700\",\"tempDegreesF\":32.584371163036465}";
        String formatted = JsonUtilities.getFormattedJsonString(unformatted);
        logger.trace("Formatted JSON: \n{}", formatted);
        Assertions.assertThat(unformatted).doesNotContain("\n");
        Assertions.assertThat(formatted).contains("\n");
        Assertions.assertThat(formatted.split("\n").length).isEqualTo(7);
        Assertions.assertThat(formatted).contains("deviceId");
        Assertions.assertThat(formatted).contains("temp1");
        Assertions.assertThat(formatted).contains("poweredOn");
        Assertions.assertThat(formatted).contains("true");
        Assertions.assertThat(formatted).contains("batteryPercent");
        Assertions.assertThat(formatted).contains("92.62947");
        Assertions.assertThat(formatted).contains("timestamp");
        Assertions.assertThat(formatted).contains("2022-02-14T11:13:55.291-0700");
        Assertions.assertThat(formatted).contains("tempDegreesF");
        Assertions.assertThat(formatted).contains("32.584371163036465");
    }
}