# A simple MQTT to Apache Kafka Connector - ARM64 Edition

**This is a fork of the [original](https://github.com/ipa-digitools/simple-kafka-mqtt-connector) that is specifically
geared toward ARM64 compatibility.**

## Usage

### Prerequisites
Java 17 or higher

Note that you should be able to use Java 8 or 11 with no problem, but the Docker container is built with 17 and
all testing/development has been done with 17.


### Configuration

* Edit the `application.properties` file

* `kafka.host = [HOSTNAME|IP]` - IP THROUGH WHICH THE KAFKA BROKER WILL BE REACHED
* `kafka.port = [INTEGER]`
* `kafka.client.id = [STRING]`
* `mqtt.host = [HOSTNAME|IP]` - IP THROUGH WHICH THE MQTT BROKER WILL BE REACHED
* `mqtt.port = [INTEGER]`
* `mqtt.client.id = [STRING]`
* `mqtt.qos = [INTEGER]` - Quality of service for MQTT - Allowed[0,1,2]
* `topic.mapping = [list_of_topic_mapping]` - This is a semicolon-separated list that describes how topics are routed from MQTT to Kafka - (Separators `>>>` (between kafka/MQTT mappings) and `;` (between pairs)) 
  * Example: `mqttTopicA>>>kafkaTopicA;mqttTopicB>>>kafkaTopicB;mqttTopicC>>>kafkaTopicC`

### How to build

* Run: `mvn clean install`


### How to run

* Place the jar with dependencies and your edited application.properties-file in the same directory
* Open a bash or CMD in the same directory as the .jar
* Run: `java -jar simple_kafka_mqtt_connector-0.1.0-SNAPSHOT-jar-with-dependencies.jar`


## Usage with Docker

* Docker Hub [original](https://hub.docker.com/r/arthurgrigo/simple-kafka-mqtt-connector) and [this ARM64 edition](https://hub.docker.com/r/mikeoertli/kafka-mqtt-connector) (WORK IN PROGRESS)
* Run (edit environment variables to your needs!): 

```shell
docker run -d -t -i -e KAFKA_HOST='localhost' -e KAFKA_PORT=9092 -e KAFKA_CLIENT_ID='testing-kafka-producer-1' \
-e MQTT_HOST='localhost' -e MQTT_PORT=1883 -e MQTT_CLIENT_ID='mqtt-client-1' -e MQTT_QOS=2 \
-e TOPIC_MAPPING='robotgroup001/robot001>>>test;robotgroup001/robot002>>>test02;robotgroup001/robot003>>>test03'  \
--name kafka-mqtt-connector mikeoertli/kafka-mqtt-connector:latest
```


## Usage with Docker-Compose

* See [docker-compose-examples](docker-compose)

### Standalone

* Stand-alone container
* [docker-compose.yml](docker-compose/standalone/docker-compose.yml)
* Run: `docker-compose up -d`


### Full Stack

* Full Stack (mqtt-broker, zookeeper, kafka-broker, kafka-mqtt-connector)
* [docker-compose.yml](docker-compose/fullstack/docker-compose.yml)
* [env.list](docker-compose/fullstack/env.list)
* Place docker-compose.yml and env.list in the same directory
* Edit env.list to your needs!
* Run: `docker-compose up -d`


## License
See [LICENSE](LICENSE) file for License
