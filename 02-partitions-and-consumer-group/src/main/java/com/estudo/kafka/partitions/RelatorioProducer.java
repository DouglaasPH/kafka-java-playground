package com.estudo.kafka.partitions;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class RelatorioProducer {
    private static final String TOPIC = "fila.relatorios";

    public static void main(String[] args) {
        var props = new Properties();
        props.put("bootstrap.servers", "localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        for (int i = 1; i <= 4; i++) {
            sendMsg(props);
        }
    }

    private static void sendMsg(Properties props) {
        try (var producer = new KafkaProducer<String, String>(props)) {
            for (int i = 1; i <= 5; i++) {
                String valor = "Relatorio de vendas #" + i;

                // Sem chave (null): o kafka distribui entre as 4 particoes
                // usando sticky partitioning (por lote, nao round-robin estrito)
                var record = new ProducerRecord<String, String>(TOPIC, valor);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println(" [x] Enviado '" + valor + "' -> particao " + metadata.partition());
                    }
                });
            }
        }
    }
}
