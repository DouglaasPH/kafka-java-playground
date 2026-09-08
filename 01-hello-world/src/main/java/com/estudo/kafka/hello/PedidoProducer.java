package com.estudo.kafka.hello;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class PedidoProducer {
    private static final String TOPIC = "pedidos.simples";

    public static void main(String[] args) {
        // Objeto de propriedades para guardar as configurações de conexão e comportamento do Producer
        Properties props = new Properties();
        // 1. Endereço dos servidores (brokers) do cluster Kafka
        props.put("bootstrap.servers", "localhost:9094");
        // 2. Serializador da Chave (Key): Converte a chave da mensagem (se houver) de String para bytes
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 3. Serializador do Valor (Value): Converte o conteúdo/corpo da mensagem de String para bytes
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Cria a instância do KafkaProducer passando as configurações
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        // Conteúdo da mensagem que queremos enviar
        String mensagem = "Novo pedido: #1001 - Notebook Gamer";

        // Cria o registro (mensagem) envelopado para o Kafka, indicando o Tópico de destino e o Conteúdo
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, mensagem);

        // Dispara o envio da mensagem para o broker Kafka de forma assíncrona
        producer.send(record);
        // Força a saída de qualquer mensagem que ainda esteja acumulada na memória
        producer.flush();
        // Encerra a conexão e libera os recursos alocados pelo Producer
        producer.close();

        // Mensagem informativa no console confirmando o envio
        System.out.println(" [x] Enviado: '" + mensagem + "'");
    }
}
