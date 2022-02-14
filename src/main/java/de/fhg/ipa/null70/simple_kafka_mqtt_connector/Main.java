package de.fhg.ipa.null70.simple_kafka_mqtt_connector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

public class Main
{

    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) //throws MqttException, InterruptedException
    {
        logger.info("######## STARTING THE SIMPLE-KAFKA-MQTT-CONNECTOR ########");
        try (SimpleKafkaMQTTConnector simpleKafkaMQTTConnector = new SimpleKafkaMQTTConnector())
        {
            CountDownLatch shutdownLatch = simpleKafkaMQTTConnector.getShutdownLatch();
            CompletableFuture.runAsync(simpleKafkaMQTTConnector);
            shutdownLatch.await();
        } catch (Exception e)
        {
            logger.error("Failure while processing MQTT client connector", e);
        }
    }
}
