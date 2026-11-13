# INF — Infra local (Docker Compose)

Referência geral: seção 9 do guia 01. Serviço de apoio: não tem linguagem, é o ambiente em que todos os outros rodam.

---

## INF-E1 — Ambiente local de desenvolvimento (Fase 0)

Subir Postgres, Kafka, Kafka UI e RabbitMQ com um comando e **entender** como Kafka e RabbitMQ se comportam antes de escrever qualquer serviço.

**Progresso:**
- [ ] INF-01 Preparar o repositório de estudo
- [ ] INF-02 Postgres com as 4 databases
- [ ] INF-03 Kafka em modo KRaft
- [ ] INF-04 Kafka UI
- [ ] INF-05 RabbitMQ com painel de gestão
- [ ] INF-06 Script de criação dos tópicos
- [ ] INF-07 Exercício manual: consumer groups no Kafka
- [ ] INF-08 Exercício manual: exchange, filas e DLQ no RabbitMQ

### INF-01 — Preparar o repositório de estudo
- **Tipo:** setup · **Fase:** 0 · **Depende de:** —
- **Contexto:** o repositório já existe com os guias. Aqui ele só agrupa os serviços por conveniência: cada serviço é autocontido e poderia ir para um repositório próprio sem ajustes. Crie a pasta `infra/` (ambiente local compartilhado) e prepare o `.gitignore` da raiz cobrindo os cinco ecossistemas (Node, Python, Java, Go, PHP) e Terraform (state, `.terraform/`, `*.tfvars` com segredo). Deixe a convenção de commits anotada.
- **Pronto quando:** `git status` não lista `node_modules`, `.venv`, `target`, `vendor`, `.terraform` nem `*.tfstate` depois que você criar projetos de exemplo temporários; `infra/` existe.
- **Referência:** guia 01, seções 7.6 e 13.

### INF-02 — Postgres com as 4 databases
- **Tipo:** infra · **Fase:** 0 · **Depende de:** INF-01
- **Contexto:** crie `infra/docker-compose.yml` com o Postgres 17 e um script em `docker-entrypoint-initdb.d` que cria `orders`, `catalog`, `payments` e `shipping`. Use volume nomeado para os dados e variáveis de ambiente para as credenciais.
- **Pronto quando:** `docker compose up -d postgres` sobe saudável; você conecta (psql ou cliente gráfico) e lista as 4 databases; `docker compose down -v && up` recria tudo.
- **Referência:** guia 01, seção 9.

### INF-03 — Kafka em modo KRaft
- **Tipo:** infra · **Fase:** 0 · **Depende de:** INF-01
- **Contexto:** adicione o Kafka (`apache/kafka`, sem Zookeeper). A parte difícil são os listeners: um endereço para clientes de fora do container (`localhost:9092`) e outro para os containers da mesma rede (`kafka:29092`). Entenda a diferença entre listener e advertised listener antes de configurar.
- **Pronto quando:** um cliente no host conecta em `localhost:9092` e um container na rede do compose conecta em `kafka:29092`; você sabe explicar por que um único endereço não serve para os dois.
- **Referência:** guia 01, seção 9.

### INF-04 — Kafka UI
- **Tipo:** infra · **Fase:** 0 · **Depende de:** INF-03
- **Contexto:** adicione a Kafka UI na porta 8090, apontada para o broker pela rede interna.
- **Pronto quando:** `http://localhost:8090` mostra o cluster, a lista de tópicos (ainda vazia), e você consegue publicar uma mensagem de teste por lá.
- **Referência:** guia 01, seção 9.

### INF-05 — RabbitMQ com painel de gestão
- **Tipo:** infra · **Fase:** 0 · **Depende de:** INF-01
- **Contexto:** adicione `rabbitmq:4-management` com as portas 5672 (AMQP) e 15672 (painel).
- **Pronto quando:** você entra em `http://localhost:15672` com guest/guest e vê a aba Exchanges com as exchanges padrão.
- **Referência:** guia 01, seção 9.

### INF-06 — Script de criação dos tópicos
- **Tipo:** infra · **Fase:** 0 · **Depende de:** INF-03
- **Contexto:** crie um script (ou um serviço `init` no compose) que usa `kafka-topics.sh` para criar os 8 tópicos da tabela 3.2 com 3 partições e replication factor 1. O script precisa ser idempotente (rodar duas vezes não quebra).
- **Pronto quando:** após `docker compose up`, a Kafka UI mostra os 8 tópicos (`order.created`, `stock.reserved`, `stock.failed`, `payment.approved`, `payment.rejected`, `order.confirmed`, `order.cancelled`, `shipment.dispatched`), cada um com 3 partições.
- **Referência:** guia 01, seções 3.2 e 9.

### INF-07 — Exercício manual: consumer groups no Kafka
- **Tipo:** exercise · **Fase:** 0 · **Depende de:** INF-04, INF-06
- **Contexto:** com `kafka-console-producer.sh` e `kafka-console-consumer.sh`, rode dois consumidores no **mesmo** group e depois em groups **diferentes**. Observe quem recebe o quê, produza com e sem chave e veja em qual partição cai, e reinicie um consumidor para ver o offset.
- **Pronto quando:** você escreve em duas linhas a diferença entre "mesmo group" e "groups diferentes", mostra na Kafka UI o lag de um consumer group, e explica por que a chave `orderId` garante ordem por pedido.
- **Referência:** guia 01, seções 1 e 9.

### INF-08 — Exercício manual: exchange, filas e DLQ no RabbitMQ
- **Tipo:** exercise · **Fase:** 0 · **Depende de:** INF-05
- **Contexto:** pelo painel, crie a exchange `notifications` (direct), a fila `notifications.email` com dead-letter para `notifications.dlx`, a fila `notifications.email.dlq` e os bindings. Publique uma mensagem na mão, faça `nack` sem requeue e veja ela cair na DLQ.
- **Pronto quando:** uma mensagem publicada com a routing key `email.send` chega em `notifications.email`; depois de rejeitada sem requeue, aparece em `notifications.email.dlq`. Você explica a diferença entre fila do RabbitMQ e consumer group do Kafka (critério de "pronto" da fase 0).
- **Referência:** guia 01, seções 1 e 3.3.

---

## INF-E2 — Sistema completo no Compose (Fase 6)

Os cinco serviços rodando em container junto da infra, com health checks, para validar o fluxo completo antes do Kubernetes.

**Progresso:**
- [ ] INF-09 Serviços HTTP no compose (order, catalog, payment)
- [ ] INF-10 Notification e shipping no compose
- [ ] INF-11 `requests.http` do fluxo completo
- [ ] INF-12 Validar os fluxos das fases 3 e 5 no compose

### INF-09 — Serviços HTTP no compose (order, catalog, payment)
- **Tipo:** infra · **Fase:** 6 · **Depende de:** ORD-34, CAT-26, PAY-19, INF-06
- **Contexto:** adicione os três serviços ao compose usando os Dockerfiles de cada um. Configuração só por variáveis de ambiente (URL do Postgres, brokers do Kafka, URL do RabbitMQ). Use `healthcheck` e `depends_on` com `condition: service_healthy` para a ordem de subida.
- **Pronto quando:** `docker compose up` sobe a infra e os três serviços; os endpoints de saúde respondem 200 (`:3000/health`, `:8000/health`, `:8080/actuator/health`); os serviços falam com o Kafka pelo endereço interno.
- **Referência:** guia 01, seções 9 e 12 (fase 6).

### INF-10 — Notification e shipping no compose
- **Tipo:** infra · **Fase:** 6 · **Depende de:** NOT-14, SHP-26, INF-09
- **Contexto:** adicione o notification e o shipping. O shipping vira **dois serviços do compose** com a mesma imagem: um para a API e outro com o comando do consumer.
- **Pronto quando:** `:8081/health` e `:8082/health` respondem 200; o worker do shipping aparece conectado como consumidor do group `shipping-service` na Kafka UI; o notification aparece como consumidor da fila `notifications.email` no painel do RabbitMQ.
- **Referência:** guia 01, seções 7.5 e 12 (fase 6).

### INF-11 — `requests.http` do fluxo completo
- **Tipo:** setup · **Fase:** 6 · **Depende de:** INF-10
- **Contexto:** monte o arquivo na raiz com variáveis de host e as chamadas: criar produtos, ajustar estoque, criar pedido (caminho feliz, sem estoque, pagamento acima de 1000, e-mail terminando em `@fail.com`), consultar pedido, consultar pagamento, consultar e despachar shipment.
- **Pronto quando:** cada cenário do roadmap tem pelo menos uma requisição pronta e comentada com o resultado esperado; o arquivo roda no REST Client do VS Code.
- **Referência:** guia 01, seção 13 (testes manuais).

### INF-12 — Validar os fluxos das fases 3 e 5 no compose
- **Tipo:** exercise · **Fase:** 6 · **Depende de:** INF-11, PAY-18, SHP-25
- **Contexto:** execute no compose o que você já validou serviço a serviço. Siga o `requests.http` e confira cada etapa na Kafka UI e no painel do RabbitMQ.
- **Pronto quando:** pedido de R$ 100 termina `CONFIRMED`, gera shipment e e-mail; pedido de R$ 2000 termina `CANCELLED` e o estoque volta; `@fail.com` vai para a DLQ após 3 tentativas; despachar o shipment publica `shipment.dispatched` e gera o e-mail `order-shipped`.
- **Referência:** guia 01, seção 12 (fases 3, 5 e 6).
