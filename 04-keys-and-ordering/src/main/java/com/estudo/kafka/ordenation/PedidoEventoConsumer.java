package com.estudo.kafka.ordenation;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PedidoEventoConsumer {
    private static final String TOPIC = "pedidos.eventos.por.cliente";

    public static void main(String[] args) {
        var props = new Properties();
        props.put("bootstrap.servers", "localhost:9094");
        props.put("group.id", "auditoria-pedidos");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        // O polling do KafkaConsumer NAO e thread-safe - uma unica thread
        // faz o poll(). Mas o PROCESSAMENTO de cada registro (potencialmente
        // lento: chamadas HTTP, IO) pode ser delegado a virtual threads,
        // sem bloquear o loop principal de poll por muito tempo.
        try (var consumer = new KafkaConsumer<String, String>(props);
             ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            System.out.println(" [*] Consumer aguardando eventos...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    executor.submit(() -> {
                        System.out.println(" [particao " + record.partition() + "] "
                                + record.key() + " -> " + record.value()
                                + " (offset=" + record.offset() + ")");
                    });
                }
            }
        }
    }
}
