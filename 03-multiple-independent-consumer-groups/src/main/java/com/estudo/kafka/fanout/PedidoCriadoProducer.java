package com.estudo.kafka.fanout;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigDecimal;
import java.util.Properties;

public class PedidoCriadoProducer {
    private static final String TOPIC = "pedidos.criados";

    public static void main(String[] args) {
        var props = new Properties();
        props.put("bootstrap.servers", "localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (var producer = new KafkaProducer<String, String>(props)) {
            var evento = new PedidoCriado("PED-2001", "Joana Silva", new BigDecimal("459.90"));

            var record = new ProducerRecord<String, String>(TOPIC, evento.pedidoId(), evento.toJson());

            producer.send(record);
            System.out.println(" [x] Evento publicado: " + evento);
        }
    }
}
