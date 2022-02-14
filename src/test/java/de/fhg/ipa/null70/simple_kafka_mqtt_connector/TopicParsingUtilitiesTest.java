package de.fhg.ipa.null70.simple_kafka_mqtt_connector;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
        List<String> kafkaFirstTopic = List.of("kafka_first");
        List<String> kafkaSecondTopic = List.of("kafka_second", "kafka_second_subtopic");
        List<String> kafkaOtherTopics = List.of("kafka_other");
        topicMap.put(mqttFirstKey, kafkaFirstTopic);
        topicMap.put(mqttSecondKey, kafkaSecondTopic);
        topicMap.put(mqttAllRegexKey, kafkaOtherTopics);

        Assertions.assertThat(TopicParsingUtilities.getMatchingKafkaTopics(mqttFirstKey, topicMap)).isEqualTo(kafkaFirstTopic);
        Assertions.assertThat(TopicParsingUtilities.getMatchingKafkaTopics(mqttSecondKey, topicMap)).isEqualTo(kafkaSecondTopic);
        Assertions.assertThat(TopicParsingUtilities.getMatchingKafkaTopics(mqttAllRegexKey, topicMap)).isEqualTo(kafkaOtherTopics);
        Assertions.assertThat(TopicParsingUtilities.getMatchingKafkaTopics("mqtt/something", topicMap)).isEqualTo(kafkaOtherTopics);
        Assertions.assertThat(TopicParsingUtilities.getMatchingKafkaTopics("mqtt/something/else", topicMap)).isEqualTo(kafkaOtherTopics);
    }

    @Test
    void getMqttRegexEntryForTopic()
    {
        Map<String, List<String>> topicMap = new HashMap<>();
        String mqttFirstKey = "mqtt/status/first";
        String mqttSecondKey = "mqtt/status/second";
        String mqttAllRegexKey = "mqtt/status/.*";
        String mqttNotRegexNoMatchKey = "mqtt/event";
        List<String> kafkaFirstTopic = List.of("kafka_first");
        List<String> kafkaOtherTopics = List.of("kafka_other");
        topicMap.put(mqttFirstKey, kafkaFirstTopic);
        topicMap.put(mqttAllRegexKey, kafkaOtherTopics);

        Optional<String> regexEntryForTopicMatch = TopicParsingUtilities.getMqttRegexEntryForTopic(mqttSecondKey, topicMap);
        Assertions.assertThat(regexEntryForTopicMatch.isPresent()).isTrue();

        Optional<String> regexEntryForNonRegexTopicMatch = TopicParsingUtilities.getMqttRegexEntryForTopic(mqttNotRegexNoMatchKey, topicMap);
        Assertions.assertThat(regexEntryForNonRegexTopicMatch.isPresent()).isFalse();
    }

    @Test
    void getMqttRegexEntryForTopic_MatchesRootOfRegexKey()
    {
        Map<String, List<String>> topicMap = new HashMap<>();
        String mqttFirstKey = "mqtt/status/first";
        String mqttAllRegexKey = "mqtt/status/.*";
        String mqttConflictWithRegexKey = "mqtt/status";
        List<String> kafkaFirstTopic = List.of("kafka_first");
        List<String> kafkaOtherTopics = List.of("kafka_other");
        topicMap.put(mqttFirstKey, kafkaFirstTopic);
        topicMap.put(mqttAllRegexKey, kafkaOtherTopics);

        Optional<String> regexEntryForNonRegexTopicMatch = TopicParsingUtilities.getMqttRegexEntryForTopic(mqttConflictWithRegexKey, topicMap);
        Assertions.assertThat(regexEntryForNonRegexTopicMatch.isPresent()).isFalse();
    }
}