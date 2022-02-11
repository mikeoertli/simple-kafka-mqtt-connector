package de.fhg.ipa.null70.simple_kafka_mqtt_connector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility functions used for parsing MQTT topics
 *
 * @since 0.1.0
 */
public class TopicParsingUtilities
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private TopicParsingUtilities()
    {
        // prevent instantiation
    }

    /**
     * Retrieves the list of kafka topics that correspond to the given MQTT topic, including the case
     * where the MQTT topic is a direct match and the case where it matches a regex mapping (i.e. map
     * all MQTT topics that start with "xyz" to a kafka topic named "something", if we get "xyz123" here
     * for the MQTT topic, this method will return a list containing "something".
     *
     * @param mqttTopic the MQTT topic to use for looking up the mapped kafka topic(s)
     * @param topicMap  the mapping of all MQTT to Kafka topics
     * @return the list of Kafka topics that map to the given MQTT topic, if any, otherwise an empty list
     */
    public static List<String> getMatchingKafkaTopics(String mqttTopic, Map<String, List<String>> topicMap)
    {
        // Check for an exact match first, then if that isn't found proceed ot regex matching
        if (topicMap.containsKey(mqttTopic))
        {
            return topicMap.get(mqttTopic);
        } else
        {
            Optional<String> regexMatch = getMqttRegexEntryForTopic(mqttTopic, topicMap);
            return regexMatch.map(topicMap::get).orElse(new ArrayList<>());
        }
    }

    /**
     * Retrieves the MQTT topic mapping defined by a regular expression for the given MQTT topic, if any exists.
     *
     * @param mqttTopic the MQTT topic of the incoming data, used to compare against the topic mapping map
     * @param topicMap  the mapping of all MQTT to Kafka topics
     * @return the topic mapping map key that is a regex which matches the provided MQTT topic
     */
    public static Optional<String> getMqttRegexEntryForTopic(String mqttTopic, Map<String, List<String>> topicMap)
    {
        for (String topic : topicMap.keySet())
        {
            if (isRegexTopic(topic))
            {
                Optional<String> correspondingRegexTopic = getCorrespondingRegexTopic(topic, mqttTopic);
                if (correspondingRegexTopic.isPresent())
                {
                    return correspondingRegexTopic;
                }
            }
        }
        return Optional.empty();
    }

    // TODO change this logic to just test for whether there is a regex match
    // the way I check for optional ispresent then return the optional (where this is called) is gross...
    private static Optional<String> getCorrespondingRegexTopic(String regexTopic, String mqttTopicToCheck)
    {
        // TODO - future logic should properly do regex matching. Right now this only supports ".*" regex topic
        //        matching similar to the Confluent connector.
        int asteriskIndex = regexTopic.indexOf(".*");

        if (asteriskIndex >= 0)
        {
            String regexTopicSubstring = regexTopic.substring(0, asteriskIndex);
            String incomingTopicSubstring = mqttTopicToCheck.substring(0, asteriskIndex);
            if (regexTopicSubstring.equalsIgnoreCase(incomingTopicSubstring))
            {
                return Optional.of(regexTopic);
            }
        }

        return Optional.empty();
    }

    /**
     * Tests whether the given topic is a regular expression. Note that this is a preliminary implementation
     * that only checks for ".*" at this time.
     *
     * @param topic the topic to test for signs of containing a regular expression
     * @return a boolean that indicates the given topic contains a regular expression
     */
    public static boolean isRegexTopic(String topic)
    {
        // this is not exhaustive, but for now it will work
        return topic.contains(".*");
    }
}
