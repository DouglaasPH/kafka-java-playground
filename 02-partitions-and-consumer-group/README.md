# Projeto 2 — Partições e Consumer Group (Kafka + Java 11)

Segundo projeto da série de estudos práticos de Apache Kafka. Aqui o objetivo é entender o **paralelismo real** do Kafka: um tópico com 4 partições, e múltiplas instâncias do mesmo consumer (mesmo `group.id`) competindo pela leitura — o equivalente conceitual ao Work Queue do RabbitMQ, mas com uma regra estrutural diferente.

---

## Objetivo de aprendizado

Depois de rodar este projeto, você deve entender:

- Que o Kafka **atribui partições inteiras** a cada consumer de um grupo — nunca divide uma partição entre dois consumers.
- Por que o paralelismo máximo de um consumer group é **limitado ao número de partições** do tópico, diferente do RabbitMQ (onde adicionar um worker sempre ajuda).
- O que é um **rebalance** e quando ele acontece.
- Como fazer **commit manual** de offset (`commitSync`) após processar cada lote.
- Como inspecionar a distribuição de partições entre consumers via linha de comando.

---

## Conceitos envolvidos

| Conceito | Onde aparece no código |
|---|---|
| Múltiplas partições | Tópico `fila.relatorios` criado com 4 partições |
| Consumer group | `group.id = "processamento-relatorios"`, compartilhado pelas instâncias |
| Rebalance | Acontece automaticamente quando um worker entra/sai do grupo |
| Commit manual de offset | `consumer.commitSync()` após processar o lote |
| Sticky partitioning (sem chave) | `ProducerRecord` publicado sem key |

> Diferente do RabbitMQ (onde qualquer worker livre pega a próxima mensagem disponível), aqui o Kafka **atribui partições inteiras e fixas** a cada consumer — a distribuição só muda quando há um rebalance (alguém entra ou sai do grupo).

---

## Estrutura do projeto

```
02-particoes-consumer-group/
├── pom.xml
├── docker-compose.yml
└── src/
    └── main/
        └── java/
            └── com/estudo/kafka/particoes/
                ├── RelatorioProducer.java
                └── RelatorioWorker.java
```

---

## Pré-requisitos

- **JDK 11**
- **Maven**
- **Docker** e **Docker Compose**

---

## Como rodar

### 1. Suba o Kafka

```bash
docker compose up -d
```

> Se já tem o Kafka de outro projeto rodando, pode reaproveitar — os tópicos têm nomes diferentes, não há conflito. Veja o README do Projeto 1 para o `docker-compose.yml` completo com os dois listeners.

### 2. Compile o projeto

```bash
mvn clean compile
```

### 3. Crie o tópico com 4 partições

```bash
docker exec -it kafka-estudo /opt/kafka/bin/kafka-topics.sh --create \
  --topic fila.relatorios \
  --partitions 4 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

### 4. Suba 3 workers (em 3 terminais separados)

```bash
mvn exec:java -Dexec.mainClass="com.estudo.kafka.particoes.RelatorioWorker"
```

Cada terminal deve mostrar:

```
 [*] Worker pronto. Aguardando particoes serem atribuidas...
```

### 5. Rode o Producer

Em um quarto terminal:

```bash
mvn exec:java -Dexec.mainClass="com.estudo.kafka.particoes.RelatorioProducer"
```

Isso publica 20 mensagens sem chave, distribuídas entre as 4 partições. Observe os 3 terminais dos workers — o Kafka atribui partições **inteiras** a cada um:

```
 [x] Particao 0 offset 3 -> Relatorio de vendas #5
 [x] Particao 1 offset 2 -> Relatorio de vendas #7
```
