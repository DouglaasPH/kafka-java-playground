package com.estudo.kafka.retry;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.StructuredTaskScope;

public class PagamentoConsumer {
    private static final String TOPIC_PRINCIPAL = "pagamentos.processar";
    private static final String TOPIC_DLQ = "pagamentos.dlq";
    private static final int MAX_TENTATIVAS = 3;

    public static void main(String[] args) {
        var consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9094");
        consumerProps.put("group.id", "processamento-pagamentos");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.offset.reset", "earliest");

        var producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9094");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (var consumer = new KafkaConsumer<String, String>(consumerProps);
             var producer = new KafkaProducer<String, String>(producerProps)) {
            consumer.subscribe(Collections.singletonList(TOPIC_PRINCIPAL));
            System.out.println(" [*] PagamentoConsumer aguardando...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    int tentativas = lerTentativas(record) + 1;

                    try {
                        boolean sucesso = processarComTimeout(record.value());

                        if (sucesso) {
                            System.out.println(" [v] Pagamento aprovado: " + record.value());
                        } else if (tentativas >= MAX_TENTATIVAS) {
                            enviarParaDlq(producer, record, tentativas);
                        } else {
                            reenviarComRetry(producer, record, tentativas);
                        }
                    } catch (Exception e) {
                        enviarParaDlq(producer, record, tentativas);
                    }
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        }
    }

    // Mesma tecnica de structured concurrency do Projeto 5 de RabbitMQ:
    // a subtarefa e o timeout sao tratados como uma unica unidade
    private static boolean processarComTimeout(String payload) throws Exception {
        try (var scope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner.<Boolean>anySuccessfulResultOrThrow(),
                cfg -> cfg.withTimeout(java.time.Duration.ofSeconds(2)))) {

            scope.fork(() -> {
                Thread.sleep(300); // simula chamada a um gateway de pagamento
                return Math.random() > 0.4; // ~60% de chance de sucesso
            });

            return scope.join();
        }
    }


    private static void reenviarComRetry(KafkaProducer<String, String> producer,
                                         ConsumerRecord<String, String> original,
                                         int tentativas) throws InterruptedException {
        // Backoff simples: espera antes de republicar. Em producao,
        // prefira topicos de espera dedicados em vez de bloquear a thread.
        Thread.sleep(2000);

        var record = new ProducerRecord<String, String>(TOPIC_PRINCIPAL, original.key(), original.value());
        record.headers().add("x-retry-count", String.valueOf(tentativas).getBytes());

        producer.send(record);
        System.out.println(" [!] Tentativa " + tentativas + " falhou, reenviado: " + original.value());
    }

    private static void enviarParaDlq(KafkaProducer<String, String> producer,
                                      ConsumerRecord<String, String> original,
                                      int tentativas) {
        var record = new ProducerRecord<String, String>(TOPIC_DLQ, original.key(), original.value());
        record.headers().add("x-retry-count", String.valueOf(tentativas).getBytes());
        record.headers().add("x-failed-reason", "max-tentativas-excedido".getBytes());

        producer.send(record);
        System.out.println(" [x] Desistindo apos " + tentativas + " tentativas, enviado a DLQ: " + original.value());
    }

    private static int lerTentativas(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader("x-retry-count");
        if (header == null) return 0;
        return Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8));
    }
}
