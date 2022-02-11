package de.fhg.ipa.null70.simple_kafka_mqtt_connector;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Verify functionality of utility methods in {@link TopicParsingUtilities}
 * 
 * @since 0.1.0
 */
class TopicParsingUtilitiesTest
{

    @Test
    void isRegexTopic()
    {
        Assertions.assertThat(TopicParsingUtilities.isRegexTopic("not/a/regex")).isFalse();
        Assertions.assertThat(TopicParsingUtilities.isRegexTopic("")).isFalse();
        Assertions.assertThat(TopicParsingUtilities.isRegexTopic("this/is/not*")).isFalse();
        Assertions.assertThat(TopicParsingUtilities.isRegexTopic("this/is/not/either.")).isFalse();
        Assertions.assertThat(TopicParsingUtilities.isRegexTopic(".*")).isTrue();
        Assertions.assertThat(TopicParsingUtilities.isRegexTopic("this/is/.*")).isTrue();
    }

    @Test
    void getMatchingKafkaTopics()
    {
        Map<String, List<String>> topicMap = new HashMap<>();
        String mqttFirstKey = "mqtt/first";
        String mqttSecondKey = "mqtt/second";
        String mqttAllRegexKey = "mqtt/.*";
        List<String> kafkaFirstTopic = List.of("kafka/first");
        List<String> kafkaSecondTopic = List.of("kafka/second", "kafka/second/subtopic");
        List<String> kafkaOtherTopics = List.of("kafka/other");
        topicMap.put(mqttFirstKey, kafkaFirstTopic);
        topicMap.put(mqttSecondKey, kafkaSecondTopic);
        topicMap.put(mqttAllRegexKey, kafkaOtherTopics);

        Assertions.assertThat(TopicParsingUtilities.getMatchingKafkaTopics(mqttFirstKey, topicMap)).isEqualTo(kafkaFirstTopic);
        Assertions.assertThat(TopicParsingUtilities.getMatchingKafkaTopics(mqttSecondKey, topicMap)).isEqualTo(kafkaSecondTopic);
        Assertions.assertThat(TopicParsingUtilities.getMatchingKafkaTopics(mqttAllRegexKey, topicMap)).isEqualTo(kafkaOtherTopics);
        Assertions.assertThat(TopicParsingUtilities.getMatchingKafkaTopics("mqtt/something", topicMap)).isEqualTo(kafkaOtherTopics);
        Assertions.assertThat(TopicParsingUtilities.getMatchingKafkaTopics("mqtt/something/else", topicMap)).isEqualTo(kafkaOtherTopics);
    }
}