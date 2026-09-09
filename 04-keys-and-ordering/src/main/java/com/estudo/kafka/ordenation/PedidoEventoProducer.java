package com.estudo.kafka.ordenation;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class PedidoEventoProducer {
    private static final String TOPIC = "pedidos.eventos.por.cliente";

    public static void main(String[] args) throws Exception {
        var props = new Properties();
        props.put("bootstrap.servers", "localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (var producer = new KafkaProducer<String, String>(props)) {
            // Todos os eventos do pedido PED-01 usam a MESMA chave:
            // garantido cair na mesma particao, na ordem publicada
            publicar(producer, "PED-01", "CRIADO");
            publicar(producer, "PED-01", "PAGAMENTO_APROVADO");
            publicar(producer, "PED-01", "SEPARADO_ESTOQUE");
            publicar(producer, "PED-01", "ENVIADO");

            // Eventos de um pedido DIFERENTE, chave diferente:
            // pode cair em outra particao, sem relacao de ordem com o PED-01
            publicar(producer, "PED-02", "CRIADO");
            publicar(producer, "PED-02", "PAGAMENTO_RECUSADO");
        }
    }

    private static void publicar(KafkaProducer<String, String> producer, String pedidoId, String status) throws Exception{
        var record = new ProducerRecord<String, String>(
                TOPIC,
                pedidoId,
                "{\"status\":\"" + status + "\"}"
        );

        var metadata = producer.send(record).get(); // .get() bloqueia so para fins didaticos
        System.out.println(" [x] " + pedidoId + " -> "
                + status+ " (particao=" + metadata.partition()
                + ", offset=" + metadata.offset() + ")"
        );
    }
}
