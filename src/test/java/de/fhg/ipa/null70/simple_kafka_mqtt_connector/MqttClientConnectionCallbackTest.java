package de.fhg.ipa.null70.simple_kafka_mqtt_connector;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.assertj.core.api.Assertions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Verify functionality of the {@link MqttClientConnectionCallback}
 *
 * @since 0.1.0
 */
class MqttClientConnectionCallbackTest
{

    @Test
    void messageArrivedUpdatesTopicMapSupplier() throws Exception
    {
        KafkaProducer<Integer, String> kafkaProducer = Mockito.mock(KafkaProducer.class);

        Map<String, List<String>> topicMap = new HashMap<>();
        String mqttFirstKey = "mqtt/first";
        String mqttSecondKey = "mqtt/second";
        String mqttAllRegexKey = "mqtt/.*";
        List<String> kafkaFirstTopic = List.of("kafka/first");
        List<String> kafkaOtherTopics = List.of("kafka/other");
        topicMap.put(mqttFirstKey, kafkaFirstTopic);
        topicMap.put(mqttAllRegexKey, kafkaOtherTopics);

        // Verify that upon the first mapping of a regex topic mapping, the full topic is added
        // to avoid the regex decode/lookup on subsequent message traffic for that MQTT topic
        Assertions.assertThat(topicMap.containsKey(mqttSecondKey)).isFalse();
        MqttClientConnectionCallback callback = new MqttClientConnectionCallback(() -> topicMap, kafkaProducer);
        callback.messageArrived(mqttSecondKey, new MqttMessage());
        Assertions.assertThat(topicMap.containsKey(mqttSecondKey)).isTrue();
    }
}