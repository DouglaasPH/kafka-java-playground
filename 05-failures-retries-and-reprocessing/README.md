# Projeto 5 — Falhas, Retry e Reprocessamento (Kafka + Java 25)

Quinto projeto da série de estudos práticos de Apache Kafka. Aqui o objetivo é lidar com falhas de processamento sem contar com nenhum mecanismo nativo do broker — diferente do RabbitMQ (que tem Dead Letter Exchange embutida), no Kafka **toda a lógica de retry e DLQ mora no seu código**.

> 📚 Parte da série [`kafka-java-playground`](#) — 7 projetos progressivos cobrindo os principais conceitos do Apache Kafka, cada um em uma versão diferente do Java (8, 11, 17, 21, 25).

---

## Objetivo de aprendizado

Depois de rodar este projeto, você deve entender:

- Por que o Kafka **não tem** um equivalente nativo à Dead Letter Exchange do RabbitMQ.
- Como implementar uma **DLQ manual**: um tópico comum, para onde você mesmo publica registros que falharam demais vezes.
- Como implementar **retry com contador de tentativas**, usando headers do registro Kafka.
- Por que, no Kafka, cada tentativa de retry é uma **mensagem nova** publicada pelo consumer — não a mesma mensagem "voltando" fisicamente, como acontece no RabbitMQ.
- Como aplicar **timeout de processamento** com structured concurrency (Java 25).

---

## Conceitos envolvidos

| Conceito | Onde aparece no código |
|---|---|
| Headers customizados | `record.headers().add("x-retry-count", ...)` |
| DLQ manual (tópico de erro) | `pagamentos.dlq` |
| Retry via republicação | `reenviarComRetry()` — publica de novo em `pagamentos.processar` |
| Backoff no código, não no broker | `Thread.sleep(2000)` antes de republicar |
| Structured concurrency + timeout | `StructuredTaskScope` em `processarComTimeout` |
| Commit de offset independente do resultado | O offset original avança mesmo em caso de falha — a tentativa seguinte é uma mensagem nova |

> Este projeto reaproveita a mesma técnica de `StructuredTaskScope` do Projeto 5 de RabbitMQ — a diferença é que aqui ela protege o processamento de cada registro Kafka, não uma mensagem AMQP.

---

## Estrutura do projeto

```
05-falhas-retry/
├── pom.xml
├── docker-compose.yml
└── src/
    └── main/
        └── java/
            └── com/estudo/kafka/retry/
                ├── PagamentoProducer.java
                └── PagamentoConsumer.java
```

---

## Entendendo o fluxo (a parte mais importante deste projeto)

```
pagamentos.processar (topico principal)
        |
        v
PagamentoConsumer tenta processar (timeout 2s)
        |
        +-- sucesso --------------------> commit do offset, segue em frente
        |
        +-- falha, tentativas < 3 ------> republica em pagamentos.processar
        |                                  com header x-retry-count + 1
        |                                  (espera Thread.sleep antes)
        |
        +-- falha, tentativas >= 3 -----> publica em pagamentos.dlq
                                            (para investigacao manual)
```

Diferente do RabbitMQ (onde uma fila de espera com TTL cronometra o atraso automaticamente), aqui **o backoff é responsabilidade do seu código**. A versão deste projeto usa um `Thread.sleep` simples; uma evolução natural seria usar tópicos de espera dedicados (`pagamentos.retry.5s`, `pagamentos.retry.30s`), cada um consumido por um serviço que aguarda e republica — evitando bloquear a thread principal do consumer.

---

## Pré-requisitos

- **JDK 25**
- **Maven** 3.9+
- **Docker** e **Docker Compose**

> Se seu ambiente ainda não tem o JDK 25, o código funciona em **Java 21** — troque `maven.compiler.release` para `21` no `pom.xml` e adicione `--enable-preview` na configuração de execução, já que `StructuredTaskScope` está em preview nessa versão.

---

## Como rodar

### 1. Suba o Kafka

```bash
docker compose up -d
```

> Veja o README do Projeto 1 para o `docker-compose.yml` completo com os dois listeners. No código Java, `bootstrap.servers` deve apontar para `localhost:9094`.

### 2. Compile o projeto

```bash
mvn clean compile
```

### 3. Crie os dois tópicos

```bash
docker exec -it kafka-estudo /opt/kafka/bin/kafka-topics.sh --create \
  --topic pagamentos.processar --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092

docker exec -it kafka-estudo /opt/kafka/bin/kafka-topics.sh --create \
  --topic pagamentos.dlq --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

### 4. Rode o Consumer

```bash
mvn exec:java -Dexec.mainClass="com.estudo.kafka.retry.PagamentoConsumer"
```

Você verá:

```
 [*] PagamentoConsumer aguardando...
```

### 5. Rode o Producer em outro terminal

```bash
mvn exec:java -Dexec.mainClass="com.estudo.kafka.retry.PagamentoProducer"
```

No terminal do **consumer**, acompanhe o ciclo de retry acontecendo (a simulação tem ~60% de chance de sucesso por tentativa):

```
 [v] Pagamento aprovado: {"pedidoId":"PED-301","valor":199.90}
 [!] Tentativa 1 falhou, reenviado: {"pedidoId":"PED-302","valor":199.90}
 [v] Pagamento aprovado: {"pedidoId":"PED-302","valor":199.90}
 [!] Tentativa 1 falhou, reenviado: {"pedidoId":"PED-305","valor":199.90}
 [!] Tentativa 2 falhou, reenviado: {"pedidoId":"PED-305","valor":199.90}
 [x] Desistindo apos 3 tentativas, enviado a DLQ: {"pedidoId":"PED-305","valor":199.90}
```

### 6. Inspecione a DLQ manualmente

```bash
docker exec -it kafka-estudo /opt/kafka/bin/kafka-console-consumer.sh \
  --topic pagamentos.dlq --from-beginning \
  --bootstrap-server localhost:9092
```
