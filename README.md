# ⚡ kafka-java-playground

**7 projetos progressivos cobrindo os principais conceitos do Apache Kafka — cada um em uma versão diferente do Java (8, 11, 17, 21, 25).**

Este repositório é a continuação direta do [`rabbitmq-java-playground`](https://github.com/DouglaasPH/rabbitmq-java-playground): a mesma metodologia de estudo — teoria em PDF, projetos pequenos e independentes, um por conceito — agora aplicada ao Apache Kafka. A ideia é sair sabendo não só *como* escrever o código, mas *por que* o modelo mental do Kafka (log distribuído, particionado, replicado) é fundamentalmente diferente do modelo de broker tradicional do RabbitMQ.

> Se você já passou pelo `rabbitmq-java-playground`, várias comparações diretas aparecem ao longo dos materiais — os dois repositórios foram desenhados para serem lidos em conjunto.

---

## Visão geral

Cada projeto:

- Tem seu **próprio `pom.xml`**, configurado para uma versão específica do JDK.
- Isola **um único conceito central** do Kafka — nada de projeto "kitchen sink" tentando mostrar tudo de uma vez.
- Compartilha o mesmo domínio de negócio (um e-commerce simples: pedidos, pagamentos, estoque) usado no repositório de RabbitMQ, para que a atenção fique 100% nos conceitos de mensageria, não em reinventar o cenário.
- Tem seu **próprio README** com explicação do fluxo, experimentos práticos sugeridos, checklist de aprendizado e troubleshooting.

Todos os 7 projetos rodam contra o **mesmo broker Kafka local** (um único `docker-compose.yml`, reaproveitado em cada pasta) — os tópicos de cada projeto têm nomes diferentes, então não há conflito entre eles.

---

## Os 3 guias em PDF

Além do código, este repositório inclui três materiais de estudo em PDF, pensados para serem lidos nesta ordem:

| # | Arquivo | O que cobre |
|---|---|---|
| 1 | `guia_tecnico_do_kafka.pdf` | Teoria completa: arquitetura (brokers, tópicos, partições, offsets), producers, consumers, consumer groups, garantias de entrega, retenção/compactação de log, e comparativo com RabbitMQ e Amazon SQS/SNS. Leia isto **antes** de mexer em qualquer código. |
| 2 | `guia_de_desenvolvimento_dos_projetos_kafka_java.pdf` | Guia prático dos 7 projetos: estrutura de pastas, `pom.xml` de cada um, código-fonte completo e checklist de teste. Use como referência enquanto codifica. |
| 3 | `guia_como_cada_projeto_kafka_funciona.pdf` | Guia visual: diagramas de arquitetura, fluxogramas numerados e tabelas de decisão para cada um dos 7 projetos — incluindo uma comparação direta, projeto a projeto, com o equivalente em RabbitMQ. |

> **Fluxo de leitura sugerido:** guia técnico (teoria) → guia visual (entendimento do fluxo) → guia de desenvolvimento + README de cada projeto (mão na massa).
>
> Os nomes de arquivo acima seguem a convenção usada no repositório de RabbitMQ — confira os nomes exatos dos PDFs na raiz deste repositório e ajuste os links se estiverem diferentes.

---

### Mapa mental de decisão

| Pergunta | Conceito |
|---|---|
| Preciso de paralelismo real dividindo o trabalho entre várias instâncias? | Múltiplas partições + Consumer Group (Projeto 2) |
| Preciso que times/serviços diferentes leiam o mesmo fluxo de eventos, cada um no seu próprio ritmo? | Múltiplos Consumer Groups (Projeto 3) |
| A ordem entre eventos relacionados importa? | Publique sempre com a mesma chave (Projeto 4) |
| Meu processamento pode falhar e preciso de um caminho de erro? | DLQ manual + retry (Projeto 5) — não é automático como no RabbitMQ |
| Preciso que ler-transformar-escrever aconteça como uma unidade atômica? | Transações (Projeto 6) |
| Meu consumer vai rodar em produção de verdade? | Idempotência por (partição, offset) + wakeup (Projeto 7) |

---

## Pré-requisitos

- **Docker** e **Docker Compose**
- **Maven** 3.6+ (o Projeto 5 recomenda 3.9+)
- Os JDKs usados pelos projetos: **8, 11, 17, 21 e 25**

---

## Como rodar

### 1. Suba o Kafka (uma vez só, para todos os projetos)

```bash
git clone https://github.com/DouglaasPH/kafka-java-playground.git
cd kafka-java-playground
docker compose up -d
```

### 2. Entre na pasta do projeto que quiser rodar

```bash
cd 01-hello-world
mvn clean compile
```

Cada pasta tem seu próprio README com o passo a passo específico de execução, incluindo os comandos exatos de criação de tópico para aquele projeto.

### 3. Pare o broker quando terminar

```bash
docker compose down       # mantém os dados
docker compose down -v    # reseta tudo (apaga tópicos e offsets)
```

---

## ️ Estrutura do repositório

```
kafka-java-playground/
├── docker-compose.yml
├── guia_tecnico_do_kafka.pdf
├── guia_de_desenvolvimento_dos_projetos_kafka_java.pdf
├── guia_como_cada_projeto_kafka_funciona.pdf
├── 01-hello-world/
├── 02-particoes-consumer-group/
├── 03-multiplos-consumer-groups/
├── 04-chaves-ordenacao/
├── 05-falhas-retry/
├── 06-transacoes/
└── 07-consumer-robusto/
```

Cada pasta de projeto segue o mesmo padrão interno:

```
0N-nome-do-projeto/
├── pom.xml
├── README.md
└── src/main/java/com/estudo/kafka/...
```

---

## Trilha de aprendizado sugerida

1. Leia o **guia técnico** (teoria de Kafka) até se sentir confortável com os conceitos de tópico, partição, offset e consumer group.
2. Siga os projetos **na ordem** (1 → 7) — cada um introduz um conceito novo em cima do anterior.
3. Para cada projeto, leia o **README específico**, depois consulte o capítulo correspondente do **guia visual** para reforçar o fluxo com diagramas.
4. Rode os **experimentos sugeridos** de cada README (quebrar a ordem de propósito, matar o processo no meio de uma transação, forçar uma mensagem até a DLQ etc.) — é neles que o aprendizado realmente fixa.
5. Use a **Kafka UI** (`http://localhost:8080`) constantemente — é a ferramenta mais valiosa para *ver* partições, offsets, consumer groups e lag em tempo real.

---

## Kafka vs. RabbitMQ: a troca fundamental

Se você vem do `rabbitmq-java-playground`, a lição mais importante de toda essa jornada é esta:

> No RabbitMQ, o broker faz o trabalho pesado de roteamento, retry e dead-lettering — você configura, e ele executa. No Kafka, o broker é "burro" de propósito (só armazena e serve um log ordenado) — quase toda essa lógica de negócio (retry, DLQ, idempotência) mora no **seu código**, não no broker.

| Critério | RabbitMQ | Kafka |
|---|---|---|
| Após consumo | Mensagem é removida da fila | Mensagem permanece no log até expirar por retenção |
| Replay de histórico | Não nativo | Nativo — é a base do modelo |
| Dead lettering | Nativo (DLX) | Manual (você implementa) |
| Múltiplos consumidores lendo tudo | Via fanout/topic (cada um com sua fila) | Nativo, via consumer groups independentes |
| Ordem garantida | Dentro de uma fila | Por partição (via chave) |

O guia técnico e o guia visual aprofundam esse comparativo em cada um dos 7 projetos.
