# Projeto 4 — Chaves e Ordenação (Kafka + Java 21)

Quarto projeto da série de estudos práticos de Apache Kafka. Aqui o objetivo é provar, na prática, algo que costuma confundir quem vem de sistemas de fila única (como o RabbitMQ): **a ordem de entrega só é garantida entre mensagens que compartilham a mesma chave**. Sem chave — ou com chaves diferentes — não existe garantia nenhuma de ordem relativa.

---

## Objetivo de aprendizado

Depois de rodar este projeto, você deve entender:

- Como o Kafka decide em qual partição um registro cai, com base no **hash da chave**.
- Por que a mesma chave sempre cai na **mesma partição** — e por que isso é o que garante ordem.
- Que, entre chaves diferentes (ou sem chave nenhuma), **não existe relação de ordem** — cada partição evolui de forma independente.
- Como usar **virtual threads** (Java 21) para processar registros de forma concorrente sem bloquear o loop principal de `poll()`.

---

## Conceitos envolvidos

| Conceito | Onde aparece no código |
|---|---|
| Particionamento por chave | `new ProducerRecord<>(TOPIC, pedidoId, payload)` — `pedidoId` é a chave |
| Garantia de ordem por partição | Todos os eventos de `PED-01` caem na mesma partição, na ordem publicada |
| Ausência de ordem entre chaves diferentes | Eventos de `PED-01` e `PED-02` podem intercalar livremente |
| Virtual threads para processamento concorrente | `Executors.newVirtualThreadPerTaskExecutor()` no consumer |
| `metadata.partition()` no retorno do envio | `producer.send(record).get()` — bloqueante, só para fins didáticos |

> O `poll()` do `KafkaConsumer` **não é thread-safe** — só uma thread pode chamá-lo. Mas o *processamento* de cada registro (potencialmente lento) pode ser delegado a virtual threads, sem bloquear a próxima chamada de `poll()`.

---

## Estrutura do projeto

```
04-chaves-ordenacao/
├── pom.xml
├── docker-compose.yml
└── src/
    └── main/
        └── java/
            └── com/estudo/kafka/ordenacao/
                ├── PedidoEventoProducer.java
                └── PedidoEventoConsumer.java
```

---

## Pré-requisitos

- **JDK 21**
- **Maven**
- **Docker** e **Docker Compose**

---

## Como rodar

### 1. Suba o Kafka

```bash
docker compose up -d
```

> Veja o README do Projeto 1 para o `docker-compose.yml` completo com os dois listeners (`PLAINTEXT` para containers, `PLAINTEXT_HOST` para o host). No código Java, `bootstrap.servers` deve apontar para `localhost:9094`.

### 2. Compile o projeto

```bash
mvn clean compile
```

### 3. Crie o tópico com múltiplas partições

O experimento só funciona com **mais de uma partição** — com uma partição só, tudo ficaria "ordenado" por acidente, mesmo sem chave:

```bash
docker exec -it kafka-estudo /opt/kafka/bin/kafka-topics.sh --create \
  --topic pedidos.eventos.por.cliente \
  --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

### 4. Rode o Consumer

```bash
mvn exec:java -Dexec.mainClass="com.estudo.kafka.ordenacao.PedidoEventoConsumer"
```

Você verá:

```
 [*] Consumer aguardando eventos...
```

### 5. Rode o Producer em outro terminal

```bash
mvn exec:java -Dexec.mainClass="com.estudo.kafka.ordenacao.PedidoEventoProducer"
```

O producer publica, em sequência, 4 eventos de `PED-01` e 2 eventos de `PED-02`. No terminal do **consumer**, observe:

```
 [particao 1] PED-01 -> {"status":"CRIADO"} (offset=0)
 [particao 1] PED-01 -> {"status":"PAGAMENTO_APROVADO"} (offset=1)
 [particao 0] PED-02 -> {"status":"CRIADO"} (offset=0)
 [particao 1] PED-01 -> {"status":"SEPARADO_ESTOQUE"} (offset=2)
 [particao 1] PED-01 -> {"status":"ENVIADO"} (offset=3)
 [particao 0] PED-02 -> {"status":"PAGAMENTO_RECUSADO"} (offset=1)
```

Repare: todos os eventos de `PED-01` aparecem na **mesma partição** (1, neste exemplo) e **na ordem exata** em que foram publicados (CRIADO → PAGAMENTO_APROVADO → SEPARADO_ESTOQUE → ENVIADO). Os eventos de `PED-02` caem em outra partição e podem aparecer intercalados no tempo com os de `PED-01` — o que é esperado e não quebra nenhuma garantia, já que são pedidos diferentes.
