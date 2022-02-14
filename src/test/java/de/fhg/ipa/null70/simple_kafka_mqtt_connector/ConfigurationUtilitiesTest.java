package de.fhg.ipa.null70.simple_kafka_mqtt_connector;

import org.apache.commons.configuration.CompositeConfiguration;
import org.assertj.core.api.Assertions;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.jupiter.api.Test;

import static de.fhg.ipa.null70.simple_kafka_mqtt_connector.ConfigurationUtilities.APPLICATION_PROPERTIES;
import static de.fhg.ipa.null70.simple_kafka_mqtt_connector.ConfigurationUtilities.KAFKA_CLIENT_ID_KEY;
import static de.fhg.ipa.null70.simple_kafka_mqtt_connector.ConfigurationUtilities.KAFKA_HOST_KEY;
import static de.fhg.ipa.null70.simple_kafka_mqtt_connector.ConfigurationUtilities.KAFKA_PORT_KEY;
import static de.fhg.ipa.null70.simple_kafka_mqtt_connector.ConfigurationUtilities.MQTT_CLIENT_ID_KEY;
import static de.fhg.ipa.null70.simple_kafka_mqtt_connector.ConfigurationUtilities.MQTT_HOST_KEY;
import static de.fhg.ipa.null70.simple_kafka_mqtt_connector.ConfigurationUtilities.MQTT_PORT_KEY;

/**
 * Unit tests to verify functionality provided in {@link ConfigurationUtilities}
 *
 * @since 0.1.0
 */
class ConfigurationUtilitiesTest
{

    @Test
    void createDefaultCompositeConfiguration()
    {
        CompositeConfiguration defaultConfig = ConfigurationUtilities.createDefaultCompositeConfiguration();
        CompositeConfiguration config = ConfigurationUtilities.createCompositeConfiguration(APPLICATION_PROPERTIES);

        Assertions.assertThat(defaultConfig).isNotNull();
        Assertions.assertThat(defaultConfig).isEqualTo(config);
    }

    @Test
    void createCompositeConfiguration()
    {
        CompositeConfiguration defaultConfig = ConfigurationUtilities.createCompositeConfiguration(APPLICATION_PROPERTIES);
        Assertions.assertThat(defaultConfig).isNotNull();
        Assertions.assertThat(defaultConfig.getString(KAFKA_HOST_KEY)).isEqualTo("localhost");
        Assertions.assertThat(defaultConfig.getInt(KAFKA_PORT_KEY)).isEqualTo(9092);
        Assertions.assertThat(defaultConfig.getString(KAFKA_CLIENT_ID_KEY)).isEqualTo("kafka-mqtt-connector-1");
        Assertions.assertThat(defaultConfig.getString(MQTT_HOST_KEY)).isEqualTo("192.168.1.156");
        Assertions.assertThat(defaultConfig.getInt(MQTT_PORT_KEY)).isEqualTo(1883);
        Assertions.assertThat(defaultConfig.getString(MQTT_CLIENT_ID_KEY)).isEqualTo("kafka-mqtt-connector-1");
    }

    @Test
    void createDefaultConnectionOptions()
    {
        MqttConnectOptions defaultConnectionOptions = ConfigurationUtilities.createDefaultConnectionOptions(2);
        Assertions.assertThat(defaultConnectionOptions).isNotNull();
        Assertions.assertThat(defaultConnectionOptions.getWillMessage()).isNotNull();
    }

    @Test
    void getWillTopic_DefaultValue()
    {
        Assertions.assertThat(ConfigurationUtilities.getWillTopic()).isEqualTo(ConfigurationUtilities.DEFAULT_MQTT_WILL_TOPIC);
    }

    @Test
    void getWillMessage_DefaultValue()
    {
        Assertions.assertThat(ConfigurationUtilities.getWillMessage()).isEqualTo(ConfigurationUtilities.DEFAULT_MQTT_WILL_MESSAGE);
    }
}