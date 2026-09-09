# Projeto 3 — Múltiplos Consumer Groups Independentes (Kafka + Java 17)

Terceiro projeto da série de estudos práticos de Apache Kafka. Aqui o objetivo é entender o "fanout nativo" do Kafka: um evento é publicado uma única vez, e **três consumer groups independentes** (e-mail, estoque, analytics) o leem por completo, cada um no seu próprio ritmo — sem exchange, sem binding, sem filas separadas para configurar.

---

## Objetivo de aprendizado

Depois de rodar este projeto, você deve entender:

- Que, no Kafka, **múltiplos consumer groups diferentes** lendo o mesmo tópico é o comportamento padrão — não exige nenhuma configuração extra de roteamento.
- Que cada consumer group mantém seu **próprio offset**, completamente independente dos demais.
- O que é **replay**: um consumer group novo pode nascer lendo todo o histórico do tópico desde o início — algo que não existe no RabbitMQ.
- Como usar **records** (Java 17) para modelar um evento de domínio imutável, reaproveitando a mesma técnica do Projeto 3 de RabbitMQ.

---

## Conceitos envolvidos

| Conceito | Onde aparece no código |
|---|---|
| Múltiplos `group.id` no mesmo tópico | `"email-service"`, `"estoque-service"`, `"analytics-service"` |
| Publicação com chave | `new ProducerRecord<>(TOPIC, evento.pedidoId(), evento.toJson())` |
| Record como evento de domínio | `record PedidoCriado(String pedidoId, String cliente, BigDecimal total)` |
| Replay de histórico | Consumer group novo + `auto.offset.reset=earliest` |

> Diferente do Projeto 2 (onde os workers **competem** pela mesma partição), aqui cada serviço tem seu próprio `group.id` — o que significa que **todos recebem uma cópia completa** do tópico, de forma independente.

---

## Estrutura do projeto

```
03-multiplos-consumer-groups/
├── pom.xml
├── docker-compose.yml
└── src/
    └── main/
        └── java/
            └── com/estudo/kafka/fanout/
                ├── PedidoCriado.java          (record — o evento)
                ├── PedidoCriadoProducer.java
                ├── EmailService.java
                ├── EstoqueService.java
                └── AnalyticsService.java
```

---

## Pré-requisitos

- **JDK 17** instalado e configurado (`java -version` deve mostrar `17.x`)
- **Maven** 3.6+
- **Docker** e **Docker Compose**

---

## Como rodar

### 1. Suba o Kafka

```bash
docker compose up -d
```

> Veja o README do Projeto 1 para o `docker-compose.yml` completo com os dois listeners (`PLAINTEXT` para containers, `PLAINTEXT_HOST` para o host).

### 2. Compile o projeto

```bash
mvn clean compile
```

### 3. Crie o tópico

```bash
docker exec -it kafka-estudo /opt/kafka/bin/kafka-topics.sh --create \
  --topic pedidos.criados \
  --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

### 4. Suba os três consumers (em três terminais separados)

```bash
mvn exec:java -Dexec.mainClass="com.estudo.kafka.fanout.EmailService"
```

```bash
mvn exec:java -Dexec.mainClass="com.estudo.kafka.fanout.EstoqueService"
```

```bash
mvn exec:java -Dexec.mainClass="com.estudo.kafka.fanout.AnalyticsService"
```

Cada terminal deve mostrar algo como:

```
 [*] EmailService aguardando eventos...
```

### 5. Rode o Producer

Em um quarto terminal:

```bash
mvn exec:java -Dexec.mainClass="com.estudo.kafka.fanout.PedidoCriadoProducer"
```

Você verá:

```
 [x] Evento publicado: PedidoCriado[pedidoId=PED-2001, cliente=Joana Silva, total=459.90]
```

E, quase simultaneamente, **os três consumers** vão reagir ao mesmo evento, cada um no seu terminal:

```
 [email] Enviando confirmacao para: {"pedidoId":"PED-2001",...}
 [estoque] ...
 [analytics] ...
```
