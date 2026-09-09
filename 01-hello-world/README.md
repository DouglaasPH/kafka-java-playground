# Projeto 1 — Hello World (Kafka + Java 8)

Primeiro projeto da série de estudos práticos de Apache Kafka. Aqui o objetivo é o mais simples possível: um **producer** publica um registro em um tópico de partição única, e um **consumer** o lê. Sem consumer group avançado, sem múltiplas partições — só para internalizar `KafkaProducer`, `KafkaConsumer`, `ProducerRecord` e `ConsumerRecords`.

---

## Objetivo de aprendizado

Depois de rodar este projeto, você deve entender:

- Como configurar e usar `KafkaProducer` e `KafkaConsumer` a partir do Java.
- Que, mesmo sem informar uma chave, o Kafka atribui automaticamente **partição** e **offset** a cada registro.
- Que uma mensagem publicada no Kafka **não é removida** ao ser lida — diferente do RabbitMQ, ela permanece no log até expirar por retenção.
- Por que reiniciar o consumer não necessariamente relê as mensagens antigas (depende de commit de offset, tema aprofundado no Projeto 2 em diante).

---

## Conceitos envolvidos

| Conceito | Onde aparece no código |
|---|---|
| `KafkaProducer` / `ProducerRecord` | `PedidoProducer.java` |
| `KafkaConsumer` / `ConsumerRecords` | `PedidoConsumer.java` |
| Serialização/Deserialização | `key.serializer`, `value.serializer`, `key.deserializer`, `value.deserializer` |
| `group.id` (consumer group) | `props.put("group.id", "grupo-hello-world")` |
| `auto.offset.reset` | Define o que fazer quando não há offset salvo para o grupo |
| Partição e offset atribuídos automaticamente | `record.partition()`, `record.offset()` no consumer |

> Este projeto **não** faz commit manual de offset (fica no padrão auto-commit). Ack manual e controle explícito de offset são o assunto central a partir do Projeto 2.

---

## 🗂Estrutura do projeto

```
01-hello-world/
├── pom.xml
├── docker-compose.yml
└── src/
    └── main/
        └── java/
            └── com/estudo/kafka/hello/
                ├── PedidoProducer.java
                └── PedidoConsumer.java
```

---

## Pré-requisitos

- **JDK 8**
- **Maven**
- **Docker** e **Docker Compose**

---

---

## Como rodar

### 1. Suba o Kafka na pasta raiz (kafka-java-playground)

```bash
docker compose up -d
```

Aguarde alguns segundos até o broker ficar saudável. Confirme pela Kafka UI: [http://localhost:8080](http://localhost:8080) — se o cluster aparecer com status verde, está pronto.

### 2. Compile o projeto

```bash
mvn clean compile
```

### 3. Crie o tópico (opcional — o Kafka cria automaticamente na primeira publicação)

```bash
docker exec -it kafka-estudo /opt/kafka/bin/kafka-topics.sh --create \
  --topic pedidos.simples \
  --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

### 4. Rode o Consumer primeiro

```bash
mvn exec:java -Dexec.mainClass="com.estudo.kafka.hello.PedidoConsumer"
```

Você verá:

```
 [*] Aguardando mensagens. CTRL+C para sair.
```

### 5. Rode o Producer em outro terminal

```bash
mvn exec:java -Dexec.mainClass="com.estudo.kafka.hello.PedidoProducer"
```

Você verá no terminal do **producer**:

```
 [x] Enviado: 'Novo pedido: #1001 - Notebook Gamer'
```

E, quase instantaneamente, no terminal do **consumer**:

```
 [x] Recebido: 'Novo pedido: #1001 - Notebook Gamer' (particao=0, offset=0)
```
