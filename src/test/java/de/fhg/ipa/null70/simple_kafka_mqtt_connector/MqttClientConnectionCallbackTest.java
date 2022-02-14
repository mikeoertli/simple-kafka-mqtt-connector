package de.fhg.ipa.null70.simple_kafka_mqtt_connector;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.assertj.core.api.Assertions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
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
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    void messageArrivedUpdatesTopicMapSupplier() throws Exception
    {
        KafkaProducer<Integer, String> kafkaProducer = Mockito.mock(KafkaProducer.class);

        Map<String, List<String>> topicMap = new HashMap<>();
        String mqttFirstKey = "mqtt/first";
        String mqttSecondKey = "mqtt/second";
        String mqttAllRegexKey = "mqtt/.*";
        List<String> kafkaFirstTopic = List.of("kafka_first");
        List<String> kafkaOtherTopics = List.of("kafka_other");
        topicMap.put(mqttFirstKey, kafkaFirstTopic);
        topicMap.put(mqttAllRegexKey, kafkaOtherTopics);

        // Verify that upon the first mapping of a regex topic mapping, the full topic is added
        // to avoid the regex decode/lookup on subsequent message traffic for that MQTT topic
        Assertions.assertThat(topicMap.containsKey(mqttSecondKey)).isFalse();
        MqttClientConnectionCallback callback = new MqttClientConnectionCallback(() -> topicMap, kafkaProducer,
                throwable -> logger.error("fail!", throwable));
        callback.messageArrived(mqttSecondKey, new MqttMessage());
        Assertions.assertThat(topicMap.containsKey(mqttSecondKey)).isTrue();
    }
}