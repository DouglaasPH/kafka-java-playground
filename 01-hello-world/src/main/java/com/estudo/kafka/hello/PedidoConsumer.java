package com.estudo.kafka.hello;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class PedidoConsumer {
    private static final String TOPIC = "pedidos.simples";

    public static void main(String[] args) {
        // Objeto para armazenar as configurações do Consumidor
        KafkaConsumer<String, String> consumer = getStringStringKafkaConsumer();
        // Inscreve o consumidor na lista de tópicos (neste caso, apenas um tópico)
        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println(" [*] Aguardando mensagens. CTRL+C para sair.");

        // Loop infinito para manter a aplicação escutando por novas mensagens continuadamente
        while (true) {
            // poll() busca um lote (batch) de mensagens disponíveis no tópico.
            // Duration.ofMillis(500) faz o código aguardar até 500ms caso não haja mensagens imediatas.
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            // Itera sobre cada mensagem individual retornada no lote recebido
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(" [x] Recebido: '" + record.value() + "'"
                        + " (particao=" + record.partition() + ", offset=" + record.offset() + ")");
            }
        }
    }

    private static KafkaConsumer<String, String> getStringStringKafkaConsumer() {
        Properties props = new Properties();
        // 1. Endereço do broker Kafka
        props.put("bootstrap.servers", "localhost:9094");
        // 2. Identificador do grupo de consumidores (Consumer Group)
        // Consumidores do mesmo grupo dividem o trabalho de leitura das partições.
        props.put("group.id", "grupo-hello-world");
        // 3. Deserializador da Chave: Converte a chave recebida em bytes de volta para String
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 4. Deserializador do Valor: Converte o conteúdo recebido em bytes de volta para String
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 5. Define o comportamento caso o consumidor não tenha um offset (registro de leitura) salvo:
        // "earliest" faz ele ler desde o início do tópico.
        props.put("auto.offset.reset", "earliest");

        // Instancia o consumidor Kafka com as configurações fornecidas
        return new KafkaConsumer<String, String>(props);
    }
}
