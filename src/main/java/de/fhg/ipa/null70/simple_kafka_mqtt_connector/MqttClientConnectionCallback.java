package de.fhg.ipa.null70.simple_kafka_mqtt_connector;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * MQTT client connection callback
 *
 * @since 0.1.0
 */
public class MqttClientConnectionCallback implements MqttCallback
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private int messageCount = 0;
    private final Supplier<Map<String, List<String>>> mqttKafkaTopicMapSupplier;
    private final KafkaProducer<Integer, String> kafkaProducer;

    public MqttClientConnectionCallback(Supplier<Map<String, List<String>>> mqttKafkaTopicMapSupplier,
                                        KafkaProducer<Integer, String> kafkaProducer)
    {
        this.mqttKafkaTopicMapSupplier = mqttKafkaTopicMapSupplier;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void connectionLost(Throwable throwable)
    {
        logger.debug("Connection lost", throwable);
    }

    @Override
    public void messageArrived(String mqttTopic, MqttMessage mqttMessage) throws Exception
    {
        // Checks through which mqtt-topic this message was sent and sends it to the pre-configured corresponding kafka topics..

        messageCount++;
        String message = new String(mqttMessage.getPayload());
        logger.trace("messageArrived [#{}] - MQTT topic: {}. \n\tMessage: {}", messageCount, mqttTopic, message);

        try
        {
            Map<String, List<String>> mqttKafkaTopicMap = mqttKafkaTopicMapSupplier.get();
            List<String> kafkaTopics = TopicParsingUtilities.getMatchingKafkaTopics(mqttTopic, mqttKafkaTopicMap);

            if (!kafkaTopics.isEmpty())
            {
                logger.trace("Relaying message from MQTT topic ({}) to {} kafka topics ({})", mqttTopic, kafkaTopics.size(),
                        String.join(", ", kafkaTopics));
                kafkaTopics.forEach(kafkaTopic -> {
                    try
                    {
                        kafkaProducer.send(new ProducerRecord<>(kafkaTopic, message));
                    } catch (Exception e)
                    {
                        logger.error("Kafka producer failed to publish data on topic: {} for MQTT topic: {}", kafkaTopic, mqttTopic, e);
                    }
                });

                /*
                 * Capture these here because if the MQTT topic matched a regex, next time we won't
                 * have to do the regex lookup, the topic will be explicitly in the mapping
                 * This will improve performance when there is a high volume of traffic for
                 * MQTT topics which are in topics only mapped by a regex mapping entry
                 */
                mqttKafkaTopicMap.putIfAbsent(mqttTopic, kafkaTopics);
                logger.trace("relay Message to kafka - " + message);
            } else
            {
                logger.trace("Ignoring message on MQTT topic: {} (no Kafka mapping)", mqttTopic);
            }
        } catch (KafkaException e)
        {
            logger.error("There seems to be an issue with the kafka connection. Currently no messages are forwarded to the kafka cluster!!!!", e);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken t)
    {
        logger.trace("deliveryComplete -- message ID: {}", t.getMessageId());
    }

    public int getMessageCount()
    {
        return messageCount;
    }
}
