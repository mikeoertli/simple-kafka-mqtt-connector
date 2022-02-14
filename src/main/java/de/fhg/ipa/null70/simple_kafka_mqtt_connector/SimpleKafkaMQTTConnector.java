package de.fhg.ipa.null70.simple_kafka_mqtt_connector;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static de.fhg.ipa.null70.simple_kafka_mqtt_connector.ConfigurationUtilities.KAFKA_CLIENT_ID_KEY;
import static de.fhg.ipa.null70.simple_kafka_mqtt_connector.ConfigurationUtilities.KAFKA_HOST_KEY;
import static de.fhg.ipa.null70.simple_kafka_mqtt_connector.ConfigurationUtilities.KAFKA_PORT_KEY;
import static de.fhg.ipa.null70.simple_kafka_mqtt_connector.ConfigurationUtilities.MQTT_CLIENT_ID_KEY;
import static de.fhg.ipa.null70.simple_kafka_mqtt_connector.ConfigurationUtilities.MQTT_HOST_KEY;
import static de.fhg.ipa.null70.simple_kafka_mqtt_connector.ConfigurationUtilities.MQTT_PORT_KEY;
import static de.fhg.ipa.null70.simple_kafka_mqtt_connector.ConfigurationUtilities.MQTT_QOS_KEY;
import static de.fhg.ipa.null70.simple_kafka_mqtt_connector.ConfigurationUtilities.MQTT_SHUTDOWN_TOPIC_KEY;
import static de.fhg.ipa.null70.simple_kafka_mqtt_connector.ConfigurationUtilities.TOPIC_MAPPING_KEY;
import static de.fhg.ipa.null70.simple_kafka_mqtt_connector.ConfigurationUtilities.createDefaultConnectionOptions;

public class SimpleKafkaMQTTConnector implements AutoCloseable, Runnable
{
    private static final Logger logger = LogManager.getLogger(SimpleKafkaMQTTConnector.class);

    // Key = mqtt-topic input , Value = kafka-topics for output
    private static final Map<String, List<String>> MQTT_KAFKA_TOPIC_MAP = new HashMap<>();

    private final String kafkaHost;
    private final String kafkaPort;
    private final String kafkaClientId;

    private final String mqttHost;
    private final String mqttPort;
    private final String mqttClientId;
    private final Integer mqttQos;

    private final String topicMapping;
    private final String shutdownTopic;

    private final KafkaProducer<Integer, String> kafkaProducer;
    private final MqttClient client;

    private Instant connectedOpenedTime;
    private final CountDownLatch shutdownLatch;
    private final AtomicBoolean shutdownInProgress = new AtomicBoolean(false);

    public SimpleKafkaMQTTConnector() throws MqttException
    {
        CompositeConfiguration config = ConfigurationUtilities.createDefaultCompositeConfiguration();
        shutdownLatch = new CountDownLatch(1);

        // Properties configuration
        kafkaHost = config.getString(KAFKA_HOST_KEY);
        kafkaPort = config.getString(KAFKA_PORT_KEY);
        kafkaClientId = config.getString(KAFKA_CLIENT_ID_KEY);

        mqttHost = config.getString(MQTT_HOST_KEY);
        mqttPort = config.getString(MQTT_PORT_KEY);
        mqttClientId = config.getString(MQTT_CLIENT_ID_KEY);
        mqttQos = Integer.parseInt(config.getString(MQTT_QOS_KEY).trim());

        topicMapping = config.getString(TOPIC_MAPPING_KEY);
        shutdownTopic = config.getString(MQTT_SHUTDOWN_TOPIC_KEY);

        logger.info("-------- APPLICATION PROPERTIES --------");
        logger.info("{} --> {}", KAFKA_HOST_KEY, kafkaHost);
        logger.info("{} --> {}", KAFKA_PORT_KEY, kafkaPort);
        logger.info("{} --> {}", MQTT_HOST_KEY, mqttHost);
        logger.info("{} --> {}", MQTT_PORT_KEY, mqttPort);
        logger.info("{} --> {}", MQTT_CLIENT_ID_KEY, mqttClientId);
        logger.info("{} --> {}", MQTT_QOS_KEY, mqttQos);
        logger.info("{} --> {}", MQTT_SHUTDOWN_TOPIC_KEY, shutdownTopic);
        logger.info("{} --> {}", TOPIC_MAPPING_KEY, topicMapping);
        logger.info("----------------------------------------\n");

        kafkaProducer = initKafkaProducer();
        client = createMqttClient();
    }

    @Override
    public void run()
    {
        // Initialize topic routing map
        initTopicsRoutingMap(topicMapping);

        // Setup and start the mqtt client
        initMqttClient();
    }

    @Override
    public void close()
    {
        if (!shutdownInProgress.getAndSet(true))
        {
            logger.info("Shutdown initiated for MQTT client (status = {})", client.isConnected() ? "CONNECTED" : "NOT CONNECTED");
            onShutdownInitiated(null);
        } else
        {
            logger.debug("Shutdown already in progress for MQTT client (status = {})", client.isConnected() ? "CONNECTED" : "NOT CONNECTED");
        }
    }

    private KafkaProducer<Integer, String> initKafkaProducer()
    {
        logger.trace("Creating Kafka Producer...");
        logger.trace("\t{} -> {}", ProducerConfig.CLIENT_ID_CONFIG, kafkaClientId);
        logger.trace("\t{} -> {}", ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost + ":" + kafkaPort);
        logger.trace("\t{} -> {}", ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        logger.trace("\t{} -> {}", ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaClientId);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost + ":" + kafkaPort);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);
        logger.trace("Kafka producer ready to produce...");
        return kafkaProducer;
    }

    public static String getMqttHostConnectionString(String host, String port)
    {
        return "tcp://" + host + ":" + port;
    }

    private void addShutdownHook(MqttClient client)
    {
        logger.debug("Configuring shutdown hook for client connection: {} [{}]", client.getClientId(),
                client.isConnected() ? "CONNECTED" : "NOT CONNECTED");
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    private MqttClient createMqttClient() throws MqttException
    {
        String mqttHostString = getMqttHostConnectionString(mqttHost, mqttPort);
        logger.debug("Beginning to initialize MQTT client. Host string = {} and client ID = {}", mqttHostString, mqttClientId);
        return new MqttClient(mqttHostString, mqttClientId);
    }

    private void initMqttClient() //throws MqttException
    {
        addShutdownHook(client);

        try
        {
            Instant startTime = Instant.now();
            client.connect(createDefaultConnectionOptions(mqttQos));

            connectedOpenedTime = Instant.now();
            logger.debug("MQTT client ({}) connection established. Duration of attempt to open connection: {}",
                    client.getClientId(), Duration.between(startTime, connectedOpenedTime));
        } catch (MqttException e)
        {
            logger.error("Failed to connect MQTT client", e);
        }

        try
        {
            logger.debug("Beginning process of subscribing to {} MQTT topics [{}].", MQTT_KAFKA_TOPIC_MAP.size(),
                    String.join(", ", MQTT_KAFKA_TOPIC_MAP.keySet()));
            // Subscribe all configured topics via mqtt
            for (String key : MQTT_KAFKA_TOPIC_MAP.keySet())
            {
                final String topicToSub;
                if (key.contains(".*"))
                {
                    topicToSub = key.replace(".*", "#");
                    logger.debug("Tweaking topic for 'subscribe' call to replace '.*' regex wildcard " +
                            "with MQTT standard wildcard of '#'. {} -> {}", key, topicToSub);
                } else
                {
                    topicToSub = key;
                }
                client.subscribe(topicToSub);
                logger.debug("Successfully subscribed MQTT client to topic: {}", topicToSub);
            }
        } catch (MqttException e)
        {
            logger.error("Failure while subscribing with MQTT client to map of topics", e);
        }

        logger.trace("BEGINNING MQTT client connection callback initialization...");
        client.setCallback(new MqttClientConnectionCallback(() -> MQTT_KAFKA_TOPIC_MAP, kafkaProducer, this::onShutdownInitiated));
        logger.trace("COMPLETED the MQTT client connection callback initialization...");
    }

    private void onShutdownInitiated(Throwable throwable)
    {
        if (throwable != null)
        {
            logger.error("Shutdown initiated due to error", throwable);
        }

        if (client.isConnected())
        {
            if (shutdownTopic != null && !shutdownTopic.isEmpty())
            {
                try
                {
                    logger.debug("Publishing client ID MQTT message to shutdown topic ({})", shutdownTopic);
                    client.publish(shutdownTopic, new MqttMessage(client.getClientId().getBytes(StandardCharsets.UTF_8)));
                } catch (MqttException e)
                {
                    logger.error("Failure while trying to publish unsubscribe message");
                }
            }
            logger.debug("MQTT-Kafka client relay was CONNECTED to MQTT broker (connection open for {}). " +
                    "Just received notice of shutdown... disconnecting...", Duration.between(connectedOpenedTime, Instant.now()));
            try
            {
                client.disconnect();
                logger.debug("MQTT-Kafka client relay has been disconnected from MQTT broker. " +
                        "Total connection duration: {}", Duration.between(connectedOpenedTime, Instant.now()));
            } catch (MqttException e)
            {
                logger.warn("Failure while trying to disconnect from MQTT", e);
            } finally
            {
                shutdownLatch.countDown();
            }
        } else
        {
            logger.debug("MQTT-Kafka client relay was NOT CONNECTED. Just received notice of shutdown... " +
                    "no action taken");
        }
        connectedOpenedTime = null;
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

    public CountDownLatch getShutdownLatch()
    {
        return shutdownLatch;
    }
}
