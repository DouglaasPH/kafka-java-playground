package com.estudo.kafka.retry;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

public class PagamentoProducer {
    private static final String TOPIC = "pagamentos.processar";

    public static void main(String[] args) {
        var props = new Properties();
        props.put("bootstrap.servers", "localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (var producer = new KafkaProducer<String, String>(props)) {
            for (int i = 1; i <= 5; i++) {
                String pedidoId = "PED-30" + i;
                String payload = "{\"pedidoId\":\"" + pedidoId + "\",\"valor\":199.90}";

                var record = new ProducerRecord<String, String>(TOPIC, pedidoId, payload);
                record.headers().add("x-retry-count", "0".getBytes());
                record.headers().add("x-message-id", UUID.randomUUID().toString().getBytes());

                producer.send(record);
                System.out.println(" [x] Pagamento enviado: " + payload);
            }
        }
    }
}
