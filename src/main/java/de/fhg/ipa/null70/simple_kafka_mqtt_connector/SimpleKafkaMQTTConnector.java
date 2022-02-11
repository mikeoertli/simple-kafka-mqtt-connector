package de.fhg.ipa.null70.simple_kafka_mqtt_connector;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SimpleKafkaMQTTConnector
{
    private static final Logger logger = LogManager.getLogger(SimpleKafkaMQTTConnector.class);

    // Key = mqtt-topic input , Value = kafka-topics for output
    private static final Map<String, List<String>> MQTT_KAFKA_TOPIC_MAP = new HashMap<>();

    public void run(String kafkaHost, String kafkaPort, String kafkaClientId, String mqttHost, String mqttPort,
                    String mqttClientId, Integer mqttQos, String topicMapping) throws MqttException
    {
        // Initialize topic routing map
        initTopicsRoutingMap(topicMapping);

        // Init and start kafka producer
        KafkaProducer<Integer, String> kafkaProducer = initKafkaProducer(kafkaHost, kafkaPort, kafkaClientId);

        // Setup and start the mqtt client
        initMqttClient(mqttHost, mqttPort, mqttClientId, mqttQos, kafkaProducer);
    }

    private KafkaProducer<Integer, String> initKafkaProducer(String kafkaHost, String kafkaPort, String kafkaClientId)
    {
        logger.trace("Creating Kafka Producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaClientId);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost + ":" + kafkaPort);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);
        logger.trace("Kafka producer ready to produce...");
        return kafkaProducer;
    }

    private void initMqttClient(String mqttHost, String mqttPort, String mqttClientId, Integer mqttQos,
                                KafkaProducer<Integer, String> kafkaProducer) throws MqttException
    {
        String mqttHostString = "tcp://" + mqttHost + ":" + mqttPort;
        logger.debug("Beginning to initialize MQTT client. Host string = {} and client ID = {}", mqttHostString, mqttClientId);
        MqttClient client = new MqttClient(mqttHostString, mqttClientId);

        MqttConnectOptions options = new MqttConnectOptions();
        // use a persistent/durable session..
        options.setCleanSession(false);

        options.setWill("will/topic", "Disconnected!".getBytes(), mqttQos, false);

        try
        {
            client.connect(options);
            logger.debug("MQTT client connection established");
        } catch (MqttException e)
        {
            logger.error("Failed to connect MQTT client", e);
        }

        try
        {
            // Subscribe all configured topics via mqtt
            for (String key : MQTT_KAFKA_TOPIC_MAP.keySet())
            {
                client.subscribe(key);
                logger.debug("Successfully subscribed MQTT client to topic: {}", key);
            }
        } catch (MqttException e)
        {
            logger.error("Failure while subscribing with MQTT client to map of topics", e);
        }

        logger.trace("MQTT client connection callback initializing...");
        client.setCallback(new MqttClientConnectionCallback(() -> MQTT_KAFKA_TOPIC_MAP, kafkaProducer));
    }

    public static void initTopicsRoutingMap(String topicMappingString)
    {
        logger.info("Setting up topic mapping (MQTT >>> Kafka) ...");
        Arrays.asList(topicMappingString.split(";")).forEach(pair -> {
                    String[] splitPair = pair.split(">>>");
                    String mqttTopic = splitPair[0];
                    String kafkaTopic = splitPair[1];
                    MQTT_KAFKA_TOPIC_MAP.computeIfAbsent(mqttTopic, k -> new ArrayList<>()).add(kafkaTopic);
                    logger.info("\t[MQTT] {} >>> [Kafka] {}", mqttTopic, kafkaTopic);
                }
        );
    }
}
