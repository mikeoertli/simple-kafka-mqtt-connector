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
import java.util.Optional;
import java.util.function.Consumer;
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
    private final Consumer<Throwable> disconnectConsumer;

    public MqttClientConnectionCallback(Supplier<Map<String, List<String>>> mqttKafkaTopicMapSupplier,
                                        KafkaProducer<Integer, String> kafkaProducer,
                                        Consumer<Throwable> disconnectConsumer)
    {
        this.mqttKafkaTopicMapSupplier = mqttKafkaTopicMapSupplier;
        this.kafkaProducer = kafkaProducer;
        this.disconnectConsumer = disconnectConsumer;
    }

    @Override
    public void connectionLost(Throwable throwable)
    {
        logger.info("MQTT Broker Connection Lost", throwable);
        disconnectConsumer.accept(throwable);
    }

    @Override
    public void messageArrived(String mqttTopic, MqttMessage mqttMessage) throws Exception
    {
        // Checks through which mqtt-topic this message was sent and sends it to the pre-configured corresponding kafka topics..

        messageCount++;
        String message = new String(mqttMessage.getPayload());
        int mqttMessageId = mqttMessage.getId();
        if (logger.isTraceEnabled())
        {
            String formattedMessage = JsonUtilities.getFormattedJsonString(message);
            logger.trace("MQTT messageArrived [#{} / ID={}] - MQTT topic: {}. Message:\n{}", messageCount,
                    mqttMessageId, mqttTopic, formattedMessage);
        }

        try
        {
            Map<String, List<String>> mqttKafkaTopicMap = mqttKafkaTopicMapSupplier.get();
            List<String> kafkaTopics = TopicParsingUtilities.getMatchingKafkaTopics(mqttTopic, mqttKafkaTopicMap);

            if (!kafkaTopics.isEmpty())
            {
                if (logger.isTraceEnabled())
                {
                    String formattedMessage = JsonUtilities.getFormattedJsonString(message);
                    logger.trace("Relaying message (ID={}) from MQTT topic ({}) to {} kafka topic(s) ({})\n\n", mqttMessageId,
                            mqttTopic, kafkaTopics.size(), String.join(", ", kafkaTopics));
                } else if (logger.isDebugEnabled())
                {
                    logger.debug("Relaying message (ID={}) from MQTT topic ({}) to {} kafka topic(s) ({})\n\n", mqttMessageId,
                            mqttTopic, kafkaTopics.size(), String.join(", ", kafkaTopics));
                }
                kafkaTopics.forEach(kafkaTopic -> {
                    try
                    {
                        kafkaProducer.send(new ProducerRecord<>(kafkaTopic, message));
                    } catch (Exception e)
                    {
                        logger.error("Kafka producer failed to publish data on topic: {} for MQTT (message ID = {}) " +
                                "topic: {}", kafkaTopic, mqttMessageId, mqttTopic, e);
                    }
                });

                /*
                 * Capture these here because if the MQTT topic matched a regex, next time we won't
                 * have to do the regex lookup, the topic will be explicitly in the mapping
                 * This will improve performance when there is a high volume of traffic for
                 * MQTT topics which are in topics only mapped by a regex mapping entry
                 */
                boolean isNewMqttTopic = !mqttKafkaTopicMap.containsKey(mqttTopic);
                List<String> replacedKeys = mqttKafkaTopicMap.putIfAbsent(mqttTopic, kafkaTopics);

                // Only print the full topic mapping if this is a new mapping or the mapping changed
                if (logger.isTraceEnabled() && (isNewMqttTopic || replacedKeys != null))
                {
                    logger.trace("MQTT to Kafka topic mapping: \n\t{}\n\n", TopicParsingUtilities.getTopicMapForPrinting(mqttKafkaTopicMap));
                }

            } else
            {
                logger.trace("Ignoring message (ID={}) on MQTT topic: {} (no Kafka mapping)", mqttMessageId, mqttTopic);
            }
        } catch (KafkaException e)
        {
            logger.error("There seems to be an issue with the kafka connection. Currently no messages are forwarded to the kafka cluster!!", e);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken t)
    {
        logger.trace("\tdeliveryComplete -- message ID: {}", t.getMessageId());
    }

    public int getMessageCount()
    {
        return messageCount;
    }
}
