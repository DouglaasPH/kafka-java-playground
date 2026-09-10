# Projeto 6 — Exactly-Once com Transações (Kafka + Java 21)

Sexto projeto da série de estudos práticos de Apache Kafka. Este é o projeto **sem equivalente direto** no guia de RabbitMQ — implementa o padrão clássico "ler de um tópico, transformar, escrever em outro" com garantia *exactly-once*, usando a **Kafka Transactions API**.

---

## Objetivo de aprendizado

Depois de rodar este projeto, você deve entender:

- Por que o padrão "ler, transformar, escrever" é arriscado sem transações — um processo que cai no meio pode duplicar ou perder dados.
- Como o Kafka garante que a **escrita no tópico de destino** e o **commit do offset de leitura** aconteçam como uma única operação atômica.
- A diferença entre `commitTransaction()` e `abortTransaction()`, e o que acontece com o offset em cada caso.
- Por que consumers que leem os tópicos de saída precisam configurar `isolation.level=read_committed`.
- Por que `transactional.id` precisa ser único e **estável** por instância — nunca gerado aleatoriamente a cada execução.

---

## Conceitos envolvidos

| Conceito | Onde aparece no código |
|---|---|
| Producer transacional | `producer.initTransactions()`, `transactional.id` fixo |
| Transação (begin/commit/abort) | `beginTransaction()`, `commitTransaction()`, `abortTransaction()` |
| Commit de offset dentro da transação | `producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata())` |
| Isolamento de leitura | `isolation.level=read_committed` no consumer de destino |
| Read-process-write atômico | Todo o laço de processamento de um lote acontece dentro de uma única transação |

> Diferente dos projetos anteriores, aqui o `PedidoValidadorTransacional` atua como **consumer e producer ao mesmo tempo** — lê de um tópico e escreve em outro(s), tudo dentro dos limites de uma transação.

---

## Estrutura do projeto

```
06-transacoes/
├── pom.xml
├── docker-compose.yml
└── src/
    └── main/
        └── java/
            └── com/estudo/kafka/transacional/
                ├── PedidoBrutoProducer.java
                └── PedidoValidadorTransacional.java
```

---

## Entendendo o fluxo (a parte mais importante deste projeto)

```
pedidos.brutos (offset N)
        |
        v
+-------------------------------------------+
|         TRANSAÇÃO (tudo ou nada)           |
|                                             |
|  1. producer.beginTransaction()            |
|  2. valida e envia para topico de saida    |
|  3. sendOffsetsToTransaction(N+1)          |
|                                             |
+-------------------------------------------+
        |
        v
4. commitTransaction() ou abortTransaction()
        |
        +--> valido:   pedidos.validados
        +--> invalido: pedidos.invalidos
```

Se o processo cair **antes** do `commitTransaction()`, a transação inteira é abortada: nada é escrito nos tópicos de saída, e o offset de leitura não avança — na próxima execução, o mesmo lote é lido e processado novamente do zero, sem duplicar nada que já tivesse sido confirmado.

---

## Pré-requisitos

- **JDK 21**
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

### 3. Crie os três tópicos

```bash
docker exec -it kafka-estudo /opt/kafka/bin/kafka-topics.sh --create \
  --topic pedidos.brutos --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092

docker exec -it kafka-estudo /opt/kafka/bin/kafka-topics.sh --create \
  --topic pedidos.validados --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092

docker exec -it kafka-estudo /opt/kafka/bin/kafka-topics.sh --create \
  --topic pedidos.invalidos --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

### 4. Rode o Validador Transacional

```bash
mvn exec:java -Dexec.mainClass="com.estudo.kafka.transacional.PedidoValidadorTransacional"
```

Você verá:

```
 [*] Validador transacional aguardando pedidos brutos...
```

### 5. Rode o Producer em outro terminal

```bash
mvn exec:java -Dexec.mainClass="com.estudo.kafka.transacional.PedidoBrutoProducer"
```

Publica 5 pedidos brutos, um deles com valor negativo de propósito (a cada 5º pedido). No terminal do **validador**:

```
 [x] PED-401 -> pedidos.validados
 [x] PED-402 -> pedidos.validados
 [x] PED-403 -> pedidos.validados
 [x] PED-404 -> pedidos.validados
 [x] PED-405 -> pedidos.invalidos
```

### 6. Confira os tópicos de saída

```bash
docker exec -it kafka-estudo /opt/kafka/bin/kafka-console-consumer.sh \
  --topic pedidos.validados --from-beginning \
  --isolation-level read_committed \
  --bootstrap-server localhost:9092
```
