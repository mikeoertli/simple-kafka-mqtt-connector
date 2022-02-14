package de.fhg.ipa.null70.simple_kafka_mqtt_connector;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utilities used for managing configuration/settings for the application.
 *
 * @since 0.1.0
 */
public class ConfigurationUtilities
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String APPLICATION_PROPERTIES = "application.properties";

    public static final String KAFKA_HOST_KEY = "kafka.host";
    public static final String KAFKA_PORT_KEY = "kafka.port";
    public static final String KAFKA_CLIENT_ID_KEY = "kafka.client.id";

    public static final String MQTT_HOST_KEY = "mqtt.host";
    public static final String MQTT_PORT_KEY = "mqtt.port";
    public static final String MQTT_CLIENT_ID_KEY = "mqtt.client.id";
    public static final String MQTT_QOS_KEY = "mqtt.qos";
    public static final String MQTT_SHUTDOWN_TOPIC_KEY = "mqtt.shutdown.topic";
    public static final String MQTT_WILL_TOPIC_KEY = "mqtt.will.topic";
    public static final String MQTT_WILL_MESSAGE_KEY = "mqtt.will.message";

    public static final String TOPIC_MAPPING_KEY = "topic.mapping";
    public static final String DEFAULT_MQTT_WILL_TOPIC = "will/topic";
    public static final String DEFAULT_MQTT_WILL_MESSAGE = "Disconnected!";

    private ConfigurationUtilities()
    {
        // prevent instantiation
    }

    public static CompositeConfiguration createCompositeConfiguration(String propertiesFileName)
    {
        // Read application.properties
        CompositeConfiguration config = new CompositeConfiguration();
        config.addConfiguration(new SystemConfiguration());

        config.addConfiguration(new PropertiesConfiguration());
        try
        {
            config.addConfiguration(new PropertiesConfiguration(propertiesFileName));
        } catch (ConfigurationException e)
        {
            logger.error("Configuration failure while processing {} config file", propertiesFileName, e);
        }

        if (logger.isTraceEnabled())
        {
            Set<String> keys = new HashSet<>();
            config.getKeys().forEachRemaining(keys::add);
            String joinedKeyValPairs = keys.stream()
                    .map(key -> key + " --> " + config.getProperty(key))
                    .collect(Collectors.joining(",\n\t"));
            logger.trace("Configuration created with system, properties, and file ({}) config values ({} total keys). \n\t{}",
                    propertiesFileName, keys.size(), joinedKeyValPairs);
        }
        return config;
    }

    public static MqttConnectOptions createDefaultConnectionOptions(int mqttQos)
    {
        MqttConnectOptions options = new MqttConnectOptions();
        // use a persistent/durable session..
        options.setCleanSession(false);

        options.setWill(getWillTopic(), getWillMessage().getBytes(), mqttQos, false);
        return options;
    }

    public static String getWillTopic()
    {
        return createDefaultCompositeConfiguration().getString(MQTT_WILL_TOPIC_KEY, DEFAULT_MQTT_WILL_TOPIC);
    }

    public static String getWillMessage()
    {
        return createDefaultCompositeConfiguration().getString(MQTT_WILL_MESSAGE_KEY, DEFAULT_MQTT_WILL_MESSAGE);
    }

    public static CompositeConfiguration createDefaultCompositeConfiguration()
    {
        return createCompositeConfiguration(APPLICATION_PROPERTIES);
    }
}
