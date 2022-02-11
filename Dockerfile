#FROM openjdk:8u312-jdk-slim
#FROM eclipse-temurin:11.0.14.1_1-jre-focal
FROM eclipse-temurin:17-jre-focal

ENV SERVER_INSTALL_FOLDER=/app/kafka-mqtt-connector/
ENV JAR_FILE_NAME=kafka-mqtt-connector-0.1.0-jar-with-dependencies.jar

# Application propertis
ENV KAFKA_HOST=localhost
ENV KAFKA_PORT=9092
ENV KAFKA_CLIENT_ID=mqtt-kafka-producer-1

ENV MQTT_HOST=localhost
ENV MQTT_PORT=1883
ENV MQTT_CLIENT_ID=mqtt-kafka-connector-client
ENV MQTT_QOS=2

ENV TOPIC_MAPPING=robotgroup001/robot001>>>test;robotgroup001/robot002>>>test02;robotgroup001/robot003>>>test03

RUN mkdir -p "${SERVER_INSTALL_FOLDER}log"

#SERVER:
ADD src/main/resources/application.properties ${SERVER_INSTALL_FOLDER}
ADD target/${JAR_FILE_NAME} ${SERVER_INSTALL_FOLDER}

ADD docker/setConfiguration.sh ${SERVER_INSTALL_FOLDER}

WORKDIR ${SERVER_INSTALL_FOLDER}


CMD bash ${SERVER_INSTALL_FOLDER}setConfiguration.sh "${SERVER_INSTALL_FOLDER}application.properties" \
                                  "$KAFKA_HOST" "$KAFKA_PORT" "$KAFKA_CLIENT_ID" \
                                  "$MQTT_HOST" "$MQTT_PORT" "$MQTT_CLIENT_ID" "$MQTT_QOS" \
                                  "$TOPIC_MAPPING" \
    && java -jar -Xmx1024m -Xms512m ${JAR_FILE_NAME}

