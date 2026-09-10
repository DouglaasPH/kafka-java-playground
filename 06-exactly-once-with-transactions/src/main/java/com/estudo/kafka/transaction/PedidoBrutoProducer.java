package com.estudo.kafka.transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class PedidoBrutoProducer {
    private static final String TOPIC = "pedidos.brutos";

    public static void main(String[] args) {
        var props = new Properties();
        props.put("bootstrap.servers", "localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (var producer = new KafkaProducer<String, String>(props)) {
            for (int i = 1; i <= 5; i++) {
                String pedidoId = "PED40" + i;
                String valor = i % 5 == 0 ? "-50.00" : "150.00"; // um valor invalido de proposito

                String payload = "{\"pedidoId\":\"" + pedidoId + "\",\"valor\":" + valor + "}";

                producer.send(new ProducerRecord<String, String>(TOPIC, pedidoId, payload));
                System.out.println(" [x] Pedido bruto enviado: " + payload);
            }
        }
    }
}
