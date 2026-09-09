package com.estudo.kafka.fanout;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService {
    private static final String TOPIC = "pedidos.criados";
    private static final String GROUP_ID = "email-service";

    public static void main(String[] args) {
        var props = new Properties();
        props.put("bootstrap.servers", "localhost:9094");
        props.put("group.id", GROUP_ID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            System.out.println(" [*] EmailService aguardando eventos...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(" [email] Enviando confirmacao para: " + record.value());
                }
            }
        }
    }
}
