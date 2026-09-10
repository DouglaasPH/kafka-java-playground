# Projeto 7 — Consumer Produtivo (Kafka + Java 17)

Sétimo e último projeto da série de estudos práticos de Apache Kafka. Aqui o objetivo é fechar o ciclo com as mesmas duas preocupações de produção do Projeto 7 de RabbitMQ — **idempotência** e **graceful shutdown** — mas usando os mecanismos idiomáticos do Kafka: o identificador natural `(partição, offset)` e `consumer.wakeup()`.

---

## Objetivo de aprendizado

Depois de rodar este projeto, você deve entender:

- Por que `(partição, offset)` é um identificador único e natural para idempotência no Kafka — diferente do `message_id` manual usado no RabbitMQ.
- Como `consumer.wakeup()` interrompe apenas a **próxima** chamada de `poll()`, nunca um processamento já em andamento.
- Por que o Kafka dispensa o enum de estados (`RODANDO`/`DRENANDO`/`PARADO`) que foi necessário no RabbitMQ — e o que isso revela sobre a diferença entre os dois modelos de consumo (callback assíncrono vs. loop síncrono).
- Como fazer commit de offset registro por registro, para minimizar reprocessamento em caso de falha.

---

## Conceitos envolvidos

| Conceito | Onde aparece no código |
|---|---|
| Idempotência por `(partição, offset)` | `RegistroIdempotencia.jaProcessado(particao, offset)` |
| Shutdown gracioso via wakeup | `consumer.wakeup()` no shutdown hook, `WakeupException` capturada no loop |
| Commit granular de offset | `consumer.commitSync(...)` após cada registro, não por lote |
| Store de idempotência em memória (didático) | `ConcurrentHashMap.newKeySet()` |

> Como o `poll()` e o processamento do lote acontecem sequencialmente na mesma thread (diferente do callback assíncrono do RabbitMQ), não existe risco de uma nova mensagem chegar "no meio" de um processamento em curso — isso simplifica bastante o shutdown em comparação com o RabbitMQ.

---

## Estrutura do projeto

```
07-consumer-robusto/
├── pom.xml
├── docker-compose.yml
└── src/
    └── main/
        └── java/
            └── com/estudo/kafka/robusto/
                ├── RegistroIdempotencia.java
                └── EstoqueConsumerRobusto.java
```

---

## Entendendo o fluxo (a parte mais importante deste projeto)

```
estoque.baixas
        |
        v
registro chega no poll()
        |
        v
  (partição, offset) já processado?
        |
   sim ─┴─ não
   │         │
   v         v
ignora    processa (800ms)
e segue   marca (partição,offset)
          commitSync(offset+1)

Em paralelo, a qualquer momento:
  Shutdown Hook -> encerrando=true -> consumer.wakeup()
  -> interrompe o PRÓXIMO poll(), nunca o processamento atual
```

| Mecanismo | RabbitMQ (Projeto 7) | Kafka (este projeto) |
|---|---|---|
| Identificador de idempotência | `message_id` definido manualmente pelo producer | `(partição, offset)` — natural, atribuído pelo broker |
| Sinalização de shutdown | Enum de estados verificado a cada mensagem | `consumer.wakeup()` interrompe apenas a espera por novos dados |
| O que acontece com o "em andamento" | Shutdown hook espera a flag `processandoAgora` voltar a `false` | Processamento em curso nunca é interrompido — `wakeup()` só afeta o próximo `poll()` |

---

## Pré-requisitos

- **JDK 17**
- **Maven** 3.6+
- **Docker** e **Docker Compose**

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

### 3. Crie o tópico

```bash
docker exec -it kafka-estudo /opt/kafka/bin/kafka-topics.sh --create \
  --topic estoque.baixas --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

### 4. Rode o Consumer

```bash
mvn exec:java -Dexec.mainClass="com.estudo.kafka.robusto.EstoqueConsumerRobusto"
```

Você verá:

```
 [*] EstoqueConsumerRobusto rodando. Ctrl+C para testar o shutdown gracioso.
```

### 5. Publique manualmente pela linha de comando

Este projeto não tem um producer dedicado — publique direto via console producer:

```bash
docker exec -it kafka-estudo /opt/kafka/bin/kafka-console-producer.sh \
  --topic estoque.baixas --bootstrap-server localhost:9092
```

No prompt que abrir, digite e pressione Enter:

```
{"produtoId":"PROD-42","quantidade":3}
```

No terminal do consumer:

```
 [x] Baixando estoque: {"produtoId":"PROD-42","quantidade":3}
```
