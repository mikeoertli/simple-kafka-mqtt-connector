package de.fhg.ipa.null70.simple_kafka_mqtt_connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * Misc utilities related to JSON parsing
 *
 * @since 0.1.0
 */
public class JsonUtilities
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private JsonUtilities()
    {
        // prevent instantiation of utility class
    }

    public static String getFormattedJsonString(String message) throws JsonProcessingException
    {
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        Object json = mapper.readValue(message, Object.class);
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
    }
}
