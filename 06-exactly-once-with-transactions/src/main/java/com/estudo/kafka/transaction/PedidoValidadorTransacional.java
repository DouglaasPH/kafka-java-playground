package com.estudo.kafka.transaction;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PedidoValidadorTransacional {
    private static final String TOPIC_ENTRADA = "pedidos.brutos";
    private static final String TOPIC_SAIDA = "pedidos.validados";

    public static void main(String[] args) {
        var consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9094");
        consumerProps.put("group.id", "validador-pedidos");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.offset.reset", "earliest"); // Le somente mensagens de transacoes ja confirmadas (commitadas)
        consumerProps.put("isolation.level", "read_committed");

        var producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9094");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("transactional.id", "validador-pedidos-tx-1"); // identificador unico e estavel

        try (var consumer = new KafkaConsumer<String, String>(consumerProps);
             var producer = new KafkaProducer<String, String>(producerProps)) {
            producer.initTransactions();
            consumer.subscribe(Collections.singletonList(TOPIC_ENTRADA));

            System.out.println(" [*] Validador transacional aguardando pedidos brutos...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) continue;

                producer.beginTransaction();
                try {
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

                    for (ConsumerRecord<String, String> record : records) {
                        boolean valido = validar(record.value());
                        String destino = valido ? TOPIC_SAIDA : "pedidos.invalidos";

                        producer.send(new ProducerRecord<String, String>(destino, record.key(), record.value()));
                        System.out.println(" [x] " + record.key() + " -> " + destino);

                        offsets.put(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1)
                        );

                    }

                    // O commit do offset de LEITURA entra na MESMA transacao
                    // da escrita nos topicos de saida - atomico, tudo ou nada
                    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                    producer.commitTransaction();
                } catch (Exception e) {
                    producer.abortTransaction();
                    System.err.println("Transacao abortada: " + e.getMessage());
                }
            }
        }
    }

    private static boolean validar(String payloadJson) {
        // regra fake: valor negativo = invalido
        return !payloadJson.contains("-");
    }
}
