package com.estudo.kafka.robust;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EstoqueConsumerRobusto {
    private static final String TOPIC = "estoque.baixas";
    private static volatile boolean encerrando = false;

    public static void main(String[] args) {
        var props = new Properties();
        props.put("bootstrap.servers", "localhost:9094");
        props.put("group.id", "estoque-consumer-robusto");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");

        var consumer = new KafkaConsumer<String, String>(props);

        // Padrao idiomatico do Kafka para shutdown gracioso: o shutdown
        // hook chama consumer.wakeup(), que interrompe um poll() em
        // andamento lancando WakeupException - SEM perder o registro
        // que ja estava sendo processado, porque o wakeup so afeta
        // a PROXIMA chamada de poll(), nao o processamento atual.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n [!] Sinal de shutdown recebido.");
            encerrando = true;
            consumer.wakeup();
        }));

        try {
            consumer.subscribe(Collections.singletonList(TOPIC));
            System.out.println(" [*] EstoqueConsumerRobusto rodando. Ctrl+C para testar o shutdown gracioso.");

            while(!encerrando) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    if (RegistroIdempotencia.jaProcessado(record.partition(), record.offset())) {
                        System.out.println(" [=] Ja processado, ignorando: particao="
                                + record.partition() + " offset=" + record.offset());
                        continue;
                    }

                    System.out.println(" [x] Baixando estoque: " + record.value());
                    simularProcessamento();

                    RegistroIdempotencia.marcarComoProcessado(record.partition(), record.offset());

                    // Commit apos CADA registro processado com sucesso -
                    // garante que, se cair logo em seguida, na pior das
                    // hipoteses reprocessa so este registro (idempotencia
                    // acima cobre esse caso)
                    consumer.commitSync(Collections.singletonMap(
                            new org.apache.kafka.common.TopicPartition(record.topic(), record.partition()),
                            new org.apache.kafka.clients.consumer.OffsetAndMetadata(record.offset() + 1)
                    ));
                }
            }
        } catch (WakeupException e) {
            // esperado durante o shutdown - nao e um erro real
            System.out.println(" [!] Poll interrompido para shutdown.");
        } finally {
            consumer.close(); // libera o consumer do grupo de forma limpa
            System.out.println(" [x] Consumer parado com seguranca.");
        }
    }

    private static void simularProcessamento() {
        try {
            Thread.sleep(800);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
