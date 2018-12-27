/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataflowdeveloper.kstream;

import com.jayway.jsonpath.JsonPath;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;


/**
 * /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bme680out
 * <p>
 * <p>
 * For Future Kafka Streams App that will process entire record, we will convert to avro and then
 * process with schema
 * <p>
 * to use schemas / avro / schema registry
 * <p>
 * see https://github.com/georgevetticaden/kafka-streams-trucking-ref-app/blob/hdf-3-3/src/main/java/hortonworks/hdf/kafkastreams/refapp/truck/microservice/JoinFilterGeoSpeedMicroService.java#L37
 * https://github.com/georgevetticaden/kafka-streams-trucking-ref-app/blob/hdf-3-3/src/main/java/hortonworks/hdf/kafkastreams/refapp/truck/microservice/AlertSpeedingDriversMicroService.java
 */
public class BME680 {

    // migrate to properties file and spring boot
    public static final String MQTT_BROKER = "tcp://princeton0.field.hortonworks.com:1883";
    public static final String TOPIC1 = "bme680";
    public static final String TOPIC = "bme680warning";
    public static final String OUTPUT_TOPIC = "bme680out";
    public static final String MQTT_CONNECTION_FAILURE = "MQTT Connection Failure";
    public static final String MQTT_FAILURE = "MQTT Failure";
    public static final String SHUTDOWN = "Shutdown";
    public static final String STREAMS_SHUTDOWN_HOOK = "streams-shutdown-hook";
    public static final String KAFKA_BOOTSTRAP = "princeton0.field.hortonworks.com:6667";
    public static final String CLIENT = "bme680client";
    public static final int CONNECTION_TIMEOUT = 10;
    public static final String MQTT_PUBLISH_FAILURE = "MQTT Publish Failure";
    public static final String TEMPERATURE_WARNING = "Temperature warning %04.2f";
    public static final int QOS = 0;

    // logging
    private Logger log = LoggerFactory.getLogger(BME680.class.getSimpleName());

    // mqtt
    private IMqttClient publisher = null;


    /**
     * start up a process
     * TODO:   migrate to spring boot
     *
     * @param args
     */
    public static void main(String[] args) {
        BME680 bme680 = new BME680();
        bme680.start();
    }

    /**
     * output values if large to mqtt
     *
     * Example value if JSON

     {
     "systemtime" : "12/19/2018 22:15:56",
     "BH1745_green" : "4.0",
     "ltr559_prox" : "0000",
     "end" : "1545275756.7",
     "uuid" : "20181220031556_e54721d6-6110-40a6-aa5c-72dbd8a8dcb2",
     "lsm303d_accelerometer" : "+00.06g : -01.01g : +00.04g",
     "imgnamep" : "images/bog_image_p_20181220031556_e54721d6-6110-40a6-aa5c-72dbd8a8dcb2.jpg",
     "cputemp" : 51.0,
     "BH1745_blue" : "9.0",
     "te" : "47.3427119255",
     "bme680_tempc" : "28.19",
     "imgname" : "images/bog_image_20181220031556_e54721d6-6110-40a6-aa5c-72dbd8a8dcb2.jpg",
     "bme680_tempf" : "82.74",
     "ltr559_lux" : "006.87",
     "memory" : 34.9,
     "VL53L1X_distance_in_mm" : 134,
     "bme680_humidity" : "23.938",
     "host" : "vid5",
     "diskusage" : "8732.7",
     "ipaddress" : "192.168.1.167",
     "bme680_pressure" : "1017.31",
     "BH1745_clear" : "10.0",
     "BH1745_red" : "0.0",
     "lsm303d_magnetometer" : "+00.04 : +00.34 : -00.10",
     "starttime" : "12/19/2018 22:15:09"
     }

     * @param key
     * @param value
     */
    public void processValues(String key, String value) {
        log.info("Key {} Value {}", key, value);

        if (!publisher.isConnected()) {
            log.warn("MQTT Server Not Available");
            return;
        }

        String mqttMessage = null;

        if (value != null) {
            Float bme680Temperature = null;
            String temperature = null;

            // we send either entire JSON string or just the temperature via kafka to kstream
            if (value.contains("{")) {
                temperature = JsonPath.read(value, "$.bme680_tempf");
            } else {
                temperature = value.trim();
            }

            bme680Temperature = Float.parseFloat(temperature);

            // just numeric value
            if (bme680Temperature != null && bme680Temperature.floatValue() > 80f) {
                mqttMessage = String.format(TEMPERATURE_WARNING, bme680Temperature.floatValue());
            }

            MqttMessage msg = new MqttMessage(mqttMessage.getBytes());
            msg.setQos(QOS);
            msg.setRetained(true);
            try {
                this.publisher.publish(TOPIC, msg);
            } catch (MqttException e) {
                log.error(MQTT_PUBLISH_FAILURE, e);
            }
        }
    }

    // bme 680
    public void start() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, CLIENT);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(TOPIC1);

        source.foreach((key, value) -> processValues(key, value));
        source.to(OUTPUT_TOPIC);

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(STREAMS_SHUTDOWN_HOOK) {
            @Override
            public void run() {
                log.error(SHUTDOWN);
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    public BME680(IMqttClient publisher) {
        super();
        this.publisher = publisher;
    }

    public BME680() {
        super();
        initMQTT();
    }

    /**
     * initialize MQTT connections
     */
    private void initMQTT() {
        String publisherId = UUID.randomUUID().toString();
        try {
            publisher = new MqttClient(MQTT_BROKER, publisherId);
        } catch (MqttException e) {
            log.error(MQTT_FAILURE, e);
        }
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(CONNECTION_TIMEOUT);
        try {
            this.publisher.connect(options);
        } catch (MqttException e) {
            log.error(MQTT_CONNECTION_FAILURE, e);
        }
    }
}