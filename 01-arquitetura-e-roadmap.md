# ShopFlow — Guia de Arquitetura e Roadmap

> Projeto de estudo: **microserviços poliglotas** com **Kafka**, **RabbitMQ** e **Kubernetes**,
> usando **DDD**, **Clean Architecture**, **TDD** e **OOP**.
> Este guia diz *o que* construir e *em que ordem*. O código, as dependências e os projetos são seus.
>
> Referência de sintaxe das linguagens: [`02-guia-linguagens.md`](./02-guia-linguagens.md)

---

## Sumário

1. [Visão geral](#1-visão-geral)
2. [Arquitetura do sistema](#2-arquitetura-do-sistema)
3. [Contratos: eventos, filas e endpoints](#3-contratos-eventos-filas-e-endpoints)
4. [Modelo de domínio (DDD)](#4-modelo-de-domínio-ddd)
5. [Conceitos-base](#5-conceitos-base)
6. [Clean Architecture: camadas e regras](#6-clean-architecture-camadas-e-regras)
7. [Estrutura de pastas por serviço](#7-estrutura-de-pastas-por-serviço)
8. [TDD: backlog de testes por serviço](#8-tdd-backlog-de-testes-por-serviço)
9. [Infra local (Docker Compose)](#9-infra-local-docker-compose)
10. [Kubernetes](#10-kubernetes)
11. [Roadmap em fases](#11-roadmap-em-fases)
12. [Convenções](#12-convenções)
13. [Referências](#13-referências)

---

## 1. Visão geral

**ShopFlow** é um e-commerce mínimo focado no fluxo de **pedido → estoque → pagamento → notificação**.
É pequeno de propósito: a complexidade está na comunicação entre serviços, não nas telas.

| Serviço | Stack | Bounded Context | Responsabilidade |
|---|---|---|---|
| `order-service` | Node.js + NestJS (TypeScript) | **Ordering** | Cria pedidos, acompanha o status, reage aos eventos de estoque e pagamento |
| `catalog-service` | Python + FastAPI | **Catalog & Inventory** | Cadastra produtos, reserva e libera estoque |
| `payment-service` | Java + Spring Boot | **Payments** | Processa o pagamento (gateway fake) |
| `notification-service` | Go | **Notification** | Consome comandos de e-mail do RabbitMQ e "envia" (log) |

### O que cada tecnologia ensina aqui

| Tecnologia | Onde aparece | O que você aprende |
|---|---|---|
| **Kafka** | Eventos de domínio entre order, catalog e payment | Tópicos, partições, chave de mensagem, consumer groups, offsets, replay, ordenação por chave, idempotência |
| **RabbitMQ** | Comando "enviar e-mail" para o notification | Exchanges, routing keys, filas, ack/nack, retry, dead-letter queue (DLQ) |
| **Kubernetes** | Deploy de tudo no `kind` | Deployment, Service, ConfigMap, Secret, Ingress, probes, HPA, operators |
| **Microserviços** | Todo o sistema | Database-per-service, saga coreografada, consistência eventual, contratos |
| **DDD** | Camada `domain` de cada serviço | Entities, Value Objects, Aggregates, Domain Events, Repositories |
| **Clean Arch** | Organização das pastas | Regra de dependência, ports & adapters, domínio sem framework |
| **TDD** | Todo código de domínio e use case | Red → Green → Refactor, test doubles, testes de integração com Testcontainers |

### Por que Kafka *e* RabbitMQ?

- **Kafka = log de eventos.** "Algo aconteceu" (`order.created`). Vários serviços podem ler, a mensagem fica guardada e dá pra reprocessar.
- **RabbitMQ = fila de tarefas.** "Faça isso" (`enviar e-mail`). Um worker consome, confirma (ack) e a mensagem some. Retry e DLQ são nativos.

---

## 2. Arquitetura do sistema

### 2.1 Visão de componentes

```
                               ┌──────────────┐
                               │   Cliente    │  (curl / .http / Insomnia)
                               └──────┬───────┘
                                      │ HTTP
                       ┌──────────────▼───────────────┐
                       │   Ingress (nginx) — k8s      │
                       │  /orders  /products /payments│
                       └───┬──────────┬───────────┬───┘
                           │          │           │
          ┌────────────────▼──┐  ┌────▼──────────┐ ┌▼─────────────────┐
          │  order-service    │  │catalog-service│ │ payment-service  │
          │  NestJS  :3000    │  │FastAPI :8000  │ │ Spring    :8080  │
          └──┬──────┬─────┬───┘  └──┬─────────┬──┘ └──┬────────────┬──┘
             │      │     │         │         │       │            │
       ┌─────▼──┐   │     │    ┌────▼───┐     │  ┌────▼───┐        │
       │orders  │   │     │    │catalog │     │  │payments│        │
       │  DB    │   │     │    │  DB    │     │  │  DB    │        │
       └────────┘   │     │    └────────┘     │  └────────┘        │
                    │     │                   │                    │
     ═══════════════▼═════╪═══════════════════▼════════════════════▼══════
     ║                      KAFKA  (eventos de domínio)                  ║
     ║  order.created  stock.reserved  stock.failed  payment.approved    ║
     ║  payment.rejected  order.confirmed  order.cancelled               ║
     ═════════════════════╪═══════════════════════════════════════════════
                          │ comando
              ┌───────────▼────────────┐
              │  RabbitMQ              │
              │  exchange: notifications│──► queue: notifications.email ──┐
              │  dlx: notifications.dlx │──► queue: notifications.email.dlq│
              └────────────────────────┘                                   │
                                                          ┌────────────────▼───┐
                                                          │notification-service│
                                                          │  Go   (worker)     │
                                                          └────────────────────┘
```

Regras:

- **Cada serviço tem seu próprio banco** (pode ser o mesmo Postgres com databases separados no dev). Nenhum serviço lê o banco do outro.
- Comunicação **síncrona** só do cliente para os serviços (REST). **Entre serviços, só assíncrona** (Kafka/RabbitMQ).
- `notification-service` não tem banco nem API de negócio, só `/health`.

### 2.2 Saga coreografada do pedido

Não existe orquestrador. Cada serviço reage a eventos e publica os seus.

**Caminho feliz:**

```
Cliente        order-service        Kafka          catalog-service     payment-service     RabbitMQ   notification
  │ POST /orders    │                 │                   │                   │                │            │
  │────────────────►│ Order(PENDING)  │                   │                   │                │            │
  │◄── 201 ─────────│──order.created─►│──────────────────►│ reserva estoque   │                │            │
  │                 │                 │◄─stock.reserved───│                   │                │            │
  │                 │                 │──────────────────────────────────────►│ cobra          │            │
  │                 │                 │◄─────────────────────payment.approved─│                │            │
  │                 │◄────────────────│                   │                   │                │            │
  │                 │ Order(CONFIRMED)│                   │                   │                │            │
  │                 │─order.confirmed►│                   │                   │                │            │
  │                 │──────────────── email.send (comando) ─────────────────────────────────►│───────────►│ log/e-mail
```

**Caminhos de falha (compensação):**

| Evento recebido | Quem reage | Ação |
|---|---|---|
| `stock.failed` | order | Order → `CANCELLED` (motivo: sem estoque), publica `order.cancelled`, manda e-mail |
| `payment.rejected` | order | Order → `CANCELLED` (motivo: pagamento recusado), publica `order.cancelled`, manda e-mail |
| `order.cancelled` | catalog | **Libera o estoque** reservado para aquele pedido (compensação) — se houver reserva |

### 2.3 Máquina de estados do pedido

```
            ┌──────────┐  payment.approved   ┌───────────┐
  criar ──► │ PENDING  │────────────────────►│ CONFIRMED │
            └────┬─────┘                     └───────────┘
                 │ stock.failed / payment.rejected
                 ▼
            ┌───────────┐
            │ CANCELLED │
            └───────────┘
```

Transições inválidas (ex.: confirmar um `CANCELLED`) devem lançar erro de domínio — teste isso.

---

## 3. Contratos: eventos, filas e endpoints

### 3.1 Envelope padrão de evento (Kafka)

Todo evento usa o mesmo envelope em JSON:

```json
{
  "eventId": "6f1c2b9e-0d0a-4a8b-9f4e-1f2d3c4b5a69",
  "eventType": "order.created",
  "version": 1,
  "occurredAt": "2026-11-03T12:00:00Z",
  "aggregateId": "ord_01J...",
  "payload": { }
}
```

- **Chave da mensagem Kafka = `orderId`** → todos os eventos do mesmo pedido caem na mesma partição → ordem garantida por pedido.
- `eventId` serve para **idempotência**: o consumidor guarda os IDs já processados e ignora repetidos (Kafka entrega *at-least-once*).

### 3.2 Tópicos Kafka

| Tópico | Produtor | Consumidor(es) | `payload` |
|---|---|---|---|
| `order.created` | order | catalog | `{ orderId, customerId, customerEmail, items: [{ productId, quantity, unitPrice }], total }` |
| `stock.reserved` | catalog | payment | `{ orderId, total }` |
| `stock.failed` | catalog | order | `{ orderId, reason, productIds: [] }` |
| `payment.approved` | payment | order | `{ orderId, paymentId, amount }` |
| `payment.rejected` | payment | order | `{ orderId, paymentId, amount, reason }` |
| `order.confirmed` | order | (nenhum por enquanto — bom para testar replay depois) | `{ orderId }` |
| `order.cancelled` | order | catalog | `{ orderId, reason }` |

Configuração sugerida: 3 partições por tópico, replication factor 1 no dev.
Consumer groups: `catalog-service`, `payment-service`, `order-service`.

> `stock.reserved` carrega o `total` para o payment não precisar consultar o order (evita chamada síncrona).

### 3.3 RabbitMQ

| Item | Valor |
|---|---|
| Exchange | `notifications` (tipo `direct`, durable) |
| Routing key | `email.send` |
| Fila | `notifications.email` (durable) com `x-dead-letter-exchange: notifications.dlx` |
| DLX | `notifications.dlx` (direct) → fila `notifications.email.dlq` |
| Produtor | order-service |
| Consumidor | notification-service (ack manual, `prefetch` = 10) |

Mensagem:

```json
{
  "messageId": "uuid",
  "to": "cliente@email.com",
  "template": "order-confirmed",
  "data": { "orderId": "ord_01J...", "total": 199.9 }
}
```

Templates: `order-confirmed`, `order-cancelled`.

**Retry:** o consumer tenta processar; se falhar, republica com header `x-retry-count + 1`. Ao passar de 3, faz `nack(requeue=false)` → mensagem vai para a DLQ.
Para testar: faça o "sender" falhar quando `to` terminar com `@fail.com`.

### 3.4 Endpoints REST

**order-service (`:3000`)**

| Método | Rota | Body / Resposta |
|---|---|---|
| `POST` | `/orders` | `{ customerId, customerEmail, items: [{ productId, quantity, unitPrice }] }` → `201 { id, status: "PENDING", total }` |
| `GET` | `/orders/:id` | `200 { id, status, items, total, cancelReason? }` / `404` |
| `GET` | `/health` | `200` |

**catalog-service (`:8000`)**

| Método | Rota | Body / Resposta |
|---|---|---|
| `POST` | `/products` | `{ name, price, stock }` → `201 { id, ... }` |
| `GET` | `/products` | lista |
| `GET` | `/products/{id}` | `200` / `404` |
| `PATCH` | `/products/{id}/stock` | `{ quantity }` → ajusta estoque (reposição) |
| `GET` | `/health` | `200` |

**payment-service (`:8080`)**

| Método | Rota | Body / Resposta |
|---|---|---|
| `GET` | `/payments?orderId=...` | `200 { id, orderId, amount, status }` / `404` |
| `GET` | `/actuator/health` | `200` |

**notification-service (`:8081`)**: só `GET /health`.

> Simplificação consciente: o `unitPrice` vem do cliente no `POST /orders`. Num sistema real o order pegaria o preço do catalog. Fica como desafio extra (fase 7).

---

## 4. Modelo de domínio (DDD)

### Ordering (order-service)

| Tipo | Nome | Regras / Invariantes |
|---|---|---|
| Aggregate Root | `Order` | Tem ≥ 1 item. Nasce `PENDING`. `confirm()` só se `PENDING`. `cancel(reason)` só se `PENDING`. Total = soma dos itens. Registra domain events internamente (`pullEvents()`) |
| Entity | `OrderItem` | `quantity > 0`, `unitPrice > 0` |
| Value Object | `Money` | Não negativo, 2 casas decimais, imutável, `add()`, `multiply()`, igualdade por valor |
| Value Object | `Email` | Formato válido |
| Value Object | `OrderStatus` | `PENDING` \| `CONFIRMED` \| `CANCELLED` |
| Domain Events | `OrderCreated`, `OrderConfirmed`, `OrderCancelled` | |
| Repository (interface) | `OrderRepository` | `save(order)`, `findById(id)` |

### Catalog & Inventory (catalog-service)

| Tipo | Nome | Regras / Invariantes |
|---|---|---|
| Aggregate Root | `Product` | `name` não vazio, `price > 0`, `stock ≥ 0`. `reserve(qty)` falha se `qty > stock`. `release(qty)` devolve |
| Aggregate Root | `Reservation` | Uma por `orderId`. Guarda itens reservados (para liberar depois). Status `ACTIVE` / `RELEASED` |
| Domain Service | `StockReservationService` | Reserva **todos os itens ou nenhum** (se um falhar, nada é reservado) |
| Value Objects | `Money`, `Quantity` | |
| Repositories | `ProductRepository`, `ReservationRepository` | |

### Payments (payment-service)

| Tipo | Nome | Regras / Invariantes |
|---|---|---|
| Aggregate Root | `Payment` | Um por `orderId` (idempotência). Status `APPROVED` / `REJECTED`. `amount > 0` |
| Value Object | `Money` (use `BigDecimal`) | |
| Port | `PaymentGateway` | `charge(orderId, amount) → GatewayResult` |
| Regra do gateway fake | | Recusa se `amount > 1000` (determinístico = fácil de testar) |

### Notification (notification-service)

| Tipo | Nome | Regras |
|---|---|---|
| Entity | `Notification` | `to` é e-mail válido, `template` conhecido |
| Port | `EmailSender` | `Send(ctx, notification) error` |
| Adapter | `LogEmailSender` | Imprime no log; falha se `to` termina com `@fail.com` |

---

## 5. Conceitos-base

### DDD (Domain-Driven Design)

| Conceito | Definição curta | Exemplo no projeto |
|---|---|---|
| **Bounded Context** | Fronteira onde um modelo tem significado único | Ordering, Catalog, Payments, Notification = 1 serviço cada |
| **Ubiquitous Language** | Mesmos termos no código e na conversa | "reservar estoque", "confirmar pedido" |
| **Entity** | Tem identidade; muda ao longo do tempo | `Order`, `Product` |
| **Value Object** | Sem identidade, imutável, igual por valor | `Money`, `Email` |
| **Aggregate** | Cluster de objetos com uma raiz que protege invariantes | `Order` + `OrderItem` (só se mexe nos itens pelo `Order`) |
| **Domain Event** | Fato do passado relevante ao negócio | `OrderCreated` |
| **Repository** | Abstração de persistência de aggregates (interface no domínio) | `OrderRepository` |
| **Domain Service** | Regra que envolve vários aggregates e não pertence a nenhum | `StockReservationService` |
| **Anti-corruption layer** | Tradução entre modelos de contextos diferentes | Consumer que converte o evento Kafka em comando do use case |

### Clean Architecture

- **Regra de dependência:** dependências apontam **para dentro**. `domain` não importa nada de `application`, `infra` ou do framework.
- **Ports & Adapters:** o núcleo define interfaces (ports); a `infra` implementa (adapters).
- O framework (Nest, FastAPI, Spring) é **detalhe**: fica nas bordas.

### TDD

1. **Red:** escreva um teste que falha (e falha pelo motivo certo).
2. **Green:** o mínimo de código para passar.
3. **Refactor:** limpe com os testes verdes.

Pirâmide do projeto:

| Nível | O que testa | Dependências | Velocidade |
|---|---|---|---|
| **Unit** | Entities, VOs, use cases | Repositório em memória, publisher fake | ms |
| **Integration** | Repositórios reais, producers/consumers | Testcontainers (Postgres, Kafka, RabbitMQ) | s |
| **E2E** | Endpoint HTTP completo | App de pé + containers | s |

Test doubles: **fake** (implementação simples real, ex.: repo em memória — prefira), **stub** (retorno fixo), **spy/mock** (verifica chamadas — use para publishers).

### OOP

| Pilar | Resumo |
|---|---|
| Encapsulamento | Estado privado; muda só por métodos que garantem invariantes (`order.confirm()`, nunca `order.status = ...`) |
| Abstração | Expor o *quê*, esconder o *como* (`PaymentGateway`) |
| Herança | Reaproveitar comportamento (`AggregateRoot` base). Use pouco |
| Polimorfismo | Várias implementações da mesma interface (repo em memória vs Postgres) |

**SOLID** em uma linha cada:

- **S** — uma classe, um motivo para mudar (um use case por classe).
- **O** — estender sem modificar (novo adapter de e-mail sem mexer no use case).
- **L** — subtipos substituíveis (repo em memória e Postgres se comportam igual).
- **I** — interfaces pequenas (`EventPublisher` só com `publish`).
- **D** — depender de abstrações (use case recebe `OrderRepository`, não `PrismaOrderRepository`).

---

## 6. Clean Architecture: camadas e regras

```
┌──────────────────────────────────────────────────────────────────┐
│ presentation/   controllers (HTTP)  ·  consumers (Kafka/RabbitMQ) │  ← entrada
├──────────────────────────────────────────────────────────────────┤
│ infra/          repositories (DB) · messaging (producers) ·       │  ← implementa ports
│                 gateways · config/DI                              │
├──────────────────────────────────────────────────────────────────┤
│ application/    use-cases · dtos · ports (interfaces de saída)    │  ← orquestra
├──────────────────────────────────────────────────────────────────┤
│ domain/         entities · value-objects · events ·               │  ← regras de negócio
│                 repositories (interfaces) · services              │     (zero framework)
└──────────────────────────────────────────────────────────────────┘
        dependências sempre apontam para BAIXO (para o domain)
```

### Quem faz o quê

| Peça | Camada | Responsabilidade | NÃO faz |
|---|---|---|---|
| **Controller** | presentation | Recebe HTTP, valida formato (DTO), chama o use case, mapeia resultado/erro para status HTTP | Regra de negócio, acesso a banco |
| **Consumer** | presentation | Recebe mensagem, desserializa, checa idempotência, chama o use case, ack/commit | Regra de negócio |
| **Use Case** | application | **Uma ação de negócio** (`CreateOrder`, `ReserveStock`). Carrega aggregates, chama métodos do domínio, salva, publica eventos | Saber de HTTP, Kafka ou SQL |
| **Service (de domínio)** | domain | Regra que envolve mais de um aggregate | Persistir, publicar |
| **Entity / VO** | domain | Garante invariantes | Saber que existe banco |
| **Repository (interface)** | domain | Contrato de persistência | — |
| **Repository (impl)** | infra | SQL/ORM, mapeia tabela ↔ entidade | Regra de negócio |
| **Publisher (impl)** | infra | Serializa e envia para Kafka/RabbitMQ | Decidir *quando* publicar |

> "Service" no NestJS/Spring costuma virar um saco de regras. Aqui, **use case** é a classe da aplicação; **service** só para regra de domínio que não cabe numa entity.

### Fluxo de um request

```
POST /orders
  → OrdersController.create(dto)
    → CreateOrderUseCase.execute(input)
        order = Order.create(...)          // domain valida invariantes
        orderRepository.save(order)        // port → PostgresOrderRepository
        eventPublisher.publish(order.pullEvents())  // port → KafkaEventPublisher
        return output
  ← 201 { id, status, total }
```

### Fluxo de uma mensagem

```
Kafka "payment.approved"
  → PaymentEventsConsumer.onApproved(msg)
      if processed(eventId) → ack e sai
      → ConfirmOrderUseCase.execute({ orderId })
          order = repo.findById(orderId)
          order.confirm()
          repo.save(order)
          publisher.publish(OrderConfirmed)
          notificationSender.send(email.send "order-confirmed")
      markProcessed(eventId)
```

---

## 7. Estrutura de pastas por serviço

Mesmo esqueleto nos quatro serviços; muda só a convenção de nomes de cada linguagem.
Os nomes de arquivo são **sugestões** para você criar conforme for implementando.

### 7.1 order-service — NestJS

```
order-service/
├── package.json
├── tsconfig.json
├── nest-cli.json
├── Dockerfile
├── src/
│   ├── main.ts                         # ponto de entrada (NestFactory)
│   ├── app.module.ts                   # módulo raiz, importa OrdersModule
│   ├── shared/
│   │   └── domain/
│   │       ├── aggregate-root.ts       # base: guarda e devolve domain events
│   │       ├── domain-event.ts
│   │       └── domain-error.ts
│   └── modules/
│       └── orders/
│           ├── orders.module.ts        # liga ports → adapters (providers)
│           ├── domain/
│           │   ├── entities/           order.entity.ts, order-item.entity.ts
│           │   ├── value-objects/      money.vo.ts, email.vo.ts, order-status.ts
│           │   ├── events/             order-created.event.ts, order-confirmed.event.ts, order-cancelled.event.ts
│           │   ├── repositories/       order.repository.ts        (abstract class = token de DI)
│           │   └── services/           (vazio até precisar)
│           ├── application/
│           │   ├── use-cases/          create-order.usecase.ts, confirm-order.usecase.ts,
│           │   │                       cancel-order.usecase.ts, get-order.usecase.ts
│           │   ├── dtos/               create-order.input.ts, order.output.ts
│           │   └── ports/              event-publisher.port.ts, notification-sender.port.ts
│           ├── infra/
│           │   ├── database/           schema (Prisma/TypeORM/Drizzle), migrations
│           │   ├── repositories/       postgres-order.repository.ts, in-memory-order.repository.ts
│           │   └── messaging/          kafka-event-publisher.ts, rabbitmq-notification-sender.ts
│           └── presentation/
│               ├── controllers/        orders.controller.ts, create-order.request.ts (class-validator)
│               └── consumers/          stock-events.consumer.ts, payment-events.consumer.ts
└── test/
    ├── unit/                           espelha src/: order.entity.spec.ts, create-order.usecase.spec.ts
    ├── integration/                    postgres-order.repository.spec.ts, kafka-event-publisher.spec.ts
    └── e2e/                            orders.e2e-spec.ts
```

Criar: `npx @nestjs/cli new order-service` · Rodar: `npm run start:dev` · Testar: `npm test` / `npm run test:e2e`

### 7.2 catalog-service — FastAPI

```
catalog-service/
├── pyproject.toml
├── Dockerfile
├── app/
│   ├── __init__.py
│   ├── main.py                         # ponto de entrada: app = FastAPI(lifespan=...)
│   ├── config.py                       # pydantic-settings (env vars)
│   ├── shared/
│   │   └── domain/                     aggregate_root.py, domain_event.py, domain_error.py
│   └── modules/
│       └── catalog/
│           ├── __init__.py
│           ├── dependencies.py         # funções Depends() que montam os use cases
│           ├── domain/
│           │   ├── entities/           product.py, reservation.py
│           │   ├── value_objects/      money.py, quantity.py
│           │   ├── events/             stock_reserved.py, stock_failed.py
│           │   ├── repositories/       product_repository.py, reservation_repository.py  (ABC)
│           │   └── services/           stock_reservation_service.py
│           ├── application/
│           │   ├── use_cases/          create_product.py, reserve_stock.py, release_stock.py, get_product.py
│           │   ├── dtos/               product_dto.py
│           │   └── ports/              event_publisher.py
│           ├── infra/
│           │   ├── database/           models.py (SQLAlchemy), session.py, migrations (Alembic)
│           │   ├── repositories/       sqlalchemy_product_repository.py, in_memory_product_repository.py
│           │   └── messaging/          kafka_event_publisher.py
│           └── presentation/
│               ├── controllers/        product_router.py, schemas.py (Pydantic request/response)
│               └── consumers/          order_events_consumer.py
└── tests/
    ├── conftest.py                     # fixtures compartilhadas
    ├── unit/                           test_product.py, test_reserve_stock.py
    ├── integration/                    test_sqlalchemy_product_repository.py
    └── e2e/                            test_products_api.py
```

> Em Python, pastas de pacote usam `snake_case` (sem hífen) e precisam de `__init__.py` para imports explícitos.

Criar: `uv init catalog-service` · Rodar: `uv run fastapi dev app/main.py` · Testar: `uv run pytest`

### 7.3 payment-service — Spring Boot

```
payment-service/
├── pom.xml                             # ou build.gradle
├── mvnw
├── Dockerfile
└── src/
    ├── main/
    │   ├── java/com/shopflow/payment/
    │   │   ├── PaymentApplication.java       # ponto de entrada (@SpringBootApplication)
    │   │   ├── domain/
    │   │   │   ├── entities/                 Payment.java
    │   │   │   ├── valueobjects/             Money.java (record), PaymentStatus.java (enum)
    │   │   │   ├── events/                   PaymentApproved.java, PaymentRejected.java (records)
    │   │   │   ├── repositories/             PaymentRepository.java (interface)
    │   │   │   └── services/
    │   │   ├── application/
    │   │   │   ├── usecases/                 ProcessPaymentUseCase.java, GetPaymentUseCase.java
    │   │   │   ├── dtos/                     ProcessPaymentInput.java, PaymentOutput.java (records)
    │   │   │   └── ports/                    PaymentGateway.java, EventPublisher.java
    │   │   ├── infra/
    │   │   │   ├── database/                 PaymentJpaEntity.java, SpringDataPaymentRepository.java
    │   │   │   ├── repositories/             JpaPaymentRepository.java (implementa PaymentRepository)
    │   │   │   ├── messaging/                KafkaEventPublisher.java
    │   │   │   ├── gateway/                  FakePaymentGateway.java
    │   │   │   └── config/                   UseCaseConfig.java (@Bean dos use cases)
    │   │   └── presentation/
    │   │       ├── controllers/              PaymentController.java, GlobalExceptionHandler.java
    │   │       └── consumers/                StockEventsConsumer.java (@KafkaListener)
    │   └── resources/
    │       └── application.yml
    └── test/java/com/shopflow/payment/
        ├── unit/                             PaymentTest.java, ProcessPaymentUseCaseTest.java
        └── integration/                      JpaPaymentRepositoryIT.java, PaymentControllerIT.java
```

> Pacotes Java não aceitam hífen → `usecases`, `valueobjects`.
> `@Entity` do JPA fica na `infra` (`PaymentJpaEntity`), **não** na entity de domínio. O repositório converte uma na outra. É mais código, mas mantém o domínio sem framework — esse é o exercício.
> Use cases sem `@Service`: são registrados como `@Bean` em `UseCaseConfig` — assim `application` não importa Spring.

Criar: [start.spring.io](https://start.spring.io) (Java 21, Maven, deps: Web, Validation, Data JPA, PostgreSQL Driver, Spring for Apache Kafka, Actuator, Testcontainers) · Rodar: `./mvnw spring-boot:run` · Testar: `./mvnw test`

### 7.4 notification-service — Go

```
notification-service/
├── go.mod
├── Dockerfile
├── cmd/
│   └── notification/
│       └── main.go                     # ponto de entrada: config, DI manual, start consumer, graceful shutdown
└── internal/                           # internal/ = não importável por outros módulos
    ├── config/
    │   └── config.go                   # lê env vars
    └── notification/
        ├── domain/
        │   ├── notification.go         # struct Notification + NewNotification(...) (*Notification, error)
        │   └── notification_test.go
        ├── application/
        │   ├── ports.go                # interface EmailSender (definida por quem USA)
        │   ├── send_notification.go    # use case SendNotification
        │   └── send_notification_test.go
        ├── infra/
        │   ├── email/
        │   │   └── log_sender.go       # implementa EmailSender
        │   └── messaging/
        │       ├── rabbitmq_consumer.go   # consume, retry, ack/nack
        │       └── rabbitmq_consumer_test.go  (integração, build tag ou -short)
        └── presentation/
            └── http/
                └── health.go           # GET /health
```

> Em Go os testes ficam **ao lado** do código (`*_test.go`). Nome do pacote = nome da pasta.

Criar: `go mod init github.com/<seu-user>/shopflow/notification-service` · Rodar: `go run ./cmd/notification` · Testar: `go test ./...`

### 7.5 Raiz do repositório (monorepo)

```
micro-servicos/
├── 01-arquitetura-e-roadmap.md
├── 02-guia-linguagens.md
├── infra/
│   └── docker-compose.yml          # Kafka, RabbitMQ, Postgres, Kafka UI
├── k8s/
│   ├── namespace.yaml
│   ├── order/                      deployment.yaml, service.yaml, configmap.yaml, secret.yaml
│   ├── catalog/
│   ├── payment/
│   ├── notification/
│   ├── kafka/                      manifests do Strimzi (Kafka + KafkaTopic)
│   ├── rabbitmq/                   RabbitmqCluster
│   ├── postgres/
│   └── ingress.yaml
├── order-service/
├── catalog-service/
├── payment-service/
├── notification-service/
└── requests.http                   # chamadas para testar o fluxo inteiro
```

---

## 8. TDD: backlog de testes por serviço

Escreva **um teste por vez**, nesta ordem (domínio → use case → infra → e2e).

### order-service

**Domain — `Money`**
- [ ] cria com valor válido
- [ ] rejeita valor negativo
- [ ] `add` soma e retorna novo objeto (imutável)
- [ ] `multiply(qty)` funciona
- [ ] dois `Money` com mesmo valor são iguais

**Domain — `Order`**
- [ ] cria com status `PENDING` e calcula total
- [ ] rejeita pedido sem itens
- [ ] rejeita item com quantidade ≤ 0
- [ ] registra `OrderCreated` ao criar
- [ ] `confirm()` muda para `CONFIRMED` e registra `OrderConfirmed`
- [ ] `confirm()` em `CANCELLED` lança erro
- [ ] `cancel(reason)` muda para `CANCELLED` e guarda motivo
- [ ] `cancel()` em `CONFIRMED` lança erro
- [ ] `pullEvents()` devolve e limpa a lista

**Application**
- [ ] `CreateOrder` salva no repositório e publica `order.created`
- [ ] `CreateOrder` não publica nada se o domínio rejeitar
- [ ] `ConfirmOrder` confirma, publica `order.confirmed` e envia comando de e-mail
- [ ] `ConfirmOrder` com pedido inexistente → erro `OrderNotFound`
- [ ] `CancelOrder` cancela, publica `order.cancelled`, envia e-mail
- [ ] processar o mesmo evento duas vezes não muda nada (idempotência)

**Integration / E2E**
- [ ] repositório Postgres salva e recupera um `Order` com itens
- [ ] `POST /orders` → 201; body inválido → 400
- [ ] `GET /orders/:id` inexistente → 404

### catalog-service
- [ ] `Product` rejeita preço ≤ 0 e estoque negativo
- [ ] `reserve(qty)` diminui o estoque; `qty > stock` lança `InsufficientStock`
- [ ] `release(qty)` devolve estoque
- [ ] `StockReservationService` reserva todos ou nenhum
- [ ] `ReserveStock` publica `stock.reserved` quando tudo OK
- [ ] `ReserveStock` publica `stock.failed` e não altera estoque quando falta algum
- [ ] `ReserveStock` para o mesmo `orderId` duas vezes não reserva de novo
- [ ] `ReleaseStock` devolve estoque e marca reserva `RELEASED`; sem reserva → não faz nada
- [ ] `POST /products` → 201; `GET /products/{id}` inexistente → 404

### payment-service
- [ ] `Payment` aprovado/rejeitado a partir do resultado do gateway
- [ ] `FakePaymentGateway` recusa `amount > 1000`
- [ ] `ProcessPayment` salva e publica `payment.approved`
- [ ] `ProcessPayment` publica `payment.rejected` quando recusado
- [ ] `ProcessPayment` para o mesmo `orderId` não cobra duas vezes
- [ ] `GET /payments?orderId=` → 200 / 404

### notification-service
- [ ] `NewNotification` rejeita e-mail inválido e template desconhecido
- [ ] `SendNotification` chama o `EmailSender` com os dados certos
- [ ] `SendNotification` propaga erro do sender
- [ ] consumer: sucesso → ack
- [ ] consumer: falha com `retry < 3` → republica com `x-retry-count + 1`
- [ ] consumer: falha com `retry = 3` → nack sem requeue (vai para a DLQ)
- [ ] mensagem JSON inválida → direto para a DLQ (não adianta tentar de novo)

---

## 9. Infra local (Docker Compose)

Você escreve o `infra/docker-compose.yml`. Ele precisa ter:

| Serviço | Imagem sugerida | Portas | Observações |
|---|---|---|---|
| Postgres | `postgres:17` | 5432 | Crie 3 databases: `orders`, `catalog`, `payments` (script em `/docker-entrypoint-initdb.d`) |
| Kafka | `apache/kafka` (modo **KRaft**, sem Zookeeper) | 9092 | Configure `KAFKA_ADVERTISED_LISTENERS` para funcionar de fora do container (`localhost:9092`) e de dentro (`kafka:29092`) |
| Kafka UI | `provectuslabs/kafka-ui` ou `kafbat/kafka-ui` | 8090 | Ver tópicos, mensagens, consumer groups e lag |
| RabbitMQ | `rabbitmq:4-management` | 5672, 15672 | Painel em `http://localhost:15672` (guest/guest) |

Checklist:
- [ ] `docker compose up -d` sobe tudo
- [ ] Criar os tópicos (script ou `kafka-topics.sh` dentro do container) com 3 partições
- [ ] Ver os tópicos no Kafka UI
- [ ] Ver o painel do RabbitMQ
- [ ] Conectar no Postgres e ver os 3 databases

> Exercício Kafka antes de codar: use `kafka-console-producer.sh` e `kafka-console-consumer.sh` com dois consumidores no **mesmo** group e depois em groups **diferentes**. Observe a diferença.
> Exercício RabbitMQ: crie exchange, fila e binding pelo painel e publique uma mensagem na mão.

---

## 10. Kubernetes

Use **kind** (Kubernetes in Docker) localmente. Ferramentas: `kind`, `kubectl`, opcional `k9s`.

### 10.1 Conceitos que você vai usar

| Recurso | Para quê |
|---|---|
| **Namespace** | Isolar o projeto (`shopflow`) |
| **Deployment** | Mantém N réplicas de um Pod rodando; faz rolling update |
| **Service** (ClusterIP) | DNS interno estável (`order-service.shopflow.svc`) e load balancing entre pods |
| **ConfigMap** | Configuração não sensível (URLs do Kafka, nomes de tópicos) |
| **Secret** | Senhas do banco e do RabbitMQ |
| **Ingress** | Entrada HTTP única roteando por path para cada Service |
| **Probes** | `readinessProbe` (pode receber tráfego?) e `livenessProbe` (está vivo?) |
| **Resources** | `requests` / `limits` de CPU e memória |
| **HPA** | Escala réplicas por CPU (precisa do `metrics-server`) |
| **Operator / CRD** | Software que gerencia sistemas complexos (Kafka, RabbitMQ) via recursos customizados |
| **StatefulSet + PVC** | Para o Postgres (dados persistentes) |

### 10.2 Passo a passo

1. Criar cluster: `kind create cluster --name shopflow` (com config que expõe portas 80/443 para o ingress).
2. Instalar **ingress-nginx** (manifest específico para kind, ver docs do kind).
3. Instalar **Strimzi** (operator do Kafka) → criar recurso `Kafka` (modo KRaft) e `KafkaTopic` para cada tópico.
4. Instalar **RabbitMQ Cluster Operator** → criar `RabbitmqCluster`.
5. Postgres: StatefulSet + PVC + Service (ou um chart pronto — mas fazer na mão ensina mais).
6. Para cada serviço:
   - `docker build -t order-service:dev .`
   - `kind load docker-image order-service:dev --name shopflow` (sem registry)
   - `Deployment` com `imagePullPolicy: IfNotPresent`, env vindo do ConfigMap/Secret, probes, resources
   - `Service` ClusterIP
7. `Ingress` com regras `/orders` → order, `/products` → catalog, `/payments` → payment.
8. Instalar `metrics-server` e criar HPA para o order-service.

### 10.3 Exercícios de K8s

- [ ] `kubectl scale deployment catalog-service --replicas=3` → veja os 3 pods dividindo as partições do Kafka (mesmo consumer group). Com 4 réplicas, uma fica ociosa (3 partições). Por quê?
- [ ] Mate um pod (`kubectl delete pod ...`) e veja o Deployment recriar.
- [ ] Quebre a readiness probe e veja o pod sair do Service.
- [ ] Faça um rolling update trocando a imagem.
- [ ] Rode um teste de carga (k6 ou `hey`) e veja o HPA escalar.
- [ ] Derrube o notification-service, gere pedidos e suba de novo: as mensagens esperaram na fila?

---

## 11. Roadmap em fases

Cada fase tem um "pronto quando". Não avance sem fechar a anterior.

### Fase 0 — Fundamentos e infra (1–2 dias)
- [ ] Ler as seções 2–6 deste guia
- [ ] `git init` no monorepo
- [ ] Docker Compose com Postgres, Kafka, Kafka UI, RabbitMQ (seção 9)
- [ ] Fazer os exercícios manuais de Kafka e RabbitMQ

**Pronto quando:** você explica a diferença entre consumer group no Kafka e fila no RabbitMQ.

### Fase 1 — order-service sozinho (NestJS + TDD)
- [ ] Criar o projeto e montar a árvore da seção 7.1
- [ ] TDD do domínio: `Money`, `Email`, `OrderItem`, `Order` (backlog da seção 8)
- [ ] TDD dos use cases `CreateOrder` e `GetOrder` com repo **em memória** e publisher **fake**
- [ ] Controller + DTO com validação
- [ ] Repositório Postgres + teste de integração
- [ ] Testes e2e

**Pronto quando:** `POST /orders` e `GET /orders/:id` funcionam, publicador ainda é fake, todos os testes verdes.

### Fase 2 — catalog-service + Kafka (FastAPI)
- [ ] Domínio e use cases do catálogo com TDD
- [ ] CRUD de produtos
- [ ] No order: trocar o publisher fake pelo `KafkaEventPublisher` (só a infra muda!)
- [ ] No catalog: consumer de `order.created` → `ReserveStock` → publica `stock.reserved` / `stock.failed`
- [ ] Idempotência pelo `eventId` (tabela `processed_events`)

**Pronto quando:** criar um pedido baixa o estoque no catalog e você vê os eventos no Kafka UI.

### Fase 3 — payment-service + saga completa (Spring)
- [ ] Domínio, `FakePaymentGateway` e `ProcessPayment` com TDD
- [ ] Consumer de `stock.reserved` → publica `payment.approved/rejected`
- [ ] No order: consumers de `stock.failed`, `payment.approved`, `payment.rejected` → `ConfirmOrder` / `CancelOrder`
- [ ] No catalog: consumer de `order.cancelled` → `ReleaseStock` (compensação)

**Pronto quando:** pedido de R$ 100 termina `CONFIRMED`; pedido de R$ 2000 termina `CANCELLED` e o estoque volta.

### Fase 4 — notification-service + RabbitMQ (Go)
- [ ] Domínio e use case com TDD
- [ ] Consumer RabbitMQ com ack manual, retry com header e DLQ
- [ ] No order: `RabbitMQNotificationSender` publicando `email.send`
- [ ] Graceful shutdown (para de consumir, termina a mensagem atual)

**Pronto quando:** pedido confirmado gera log de e-mail; `to` com `@fail.com` termina na DLQ após 3 tentativas.

### Fase 5 — Containers
- [ ] Dockerfile **multi-stage** para cada serviço (build → imagem final pequena; Go em `distroless`/`scratch`)
- [ ] Rodar o sistema todo pelo compose (serviços + infra)
- [ ] Health checks em todos

**Pronto quando:** `docker compose up` sobe os 4 serviços e o fluxo da fase 3 funciona.

### Fase 6 — Kubernetes
- [ ] Seção 10 inteira
- [ ] Exercícios da seção 10.3

**Pronto quando:** o fluxo completo funciona chamando o Ingress e você consegue escalar o catalog.

### Fase 7 — Extras (escolha)
- [ ] **Transactional Outbox:** salvar o evento na mesma transação do aggregate e publicar depois (resolve "salvou mas não publicou")
- [ ] **Schema Registry** + Avro/Protobuf para os eventos
- [ ] Order buscar o preço no catalog (gRPC ou réplica local via eventos `product.updated`)
- [ ] **Observabilidade:** OpenTelemetry + Jaeger (trace atravessando Kafka), Prometheus + Grafana
- [ ] **CI** (GitHub Actions): testes de cada serviço em paralelo
- [ ] Kustomize ou Helm para os manifests
- [ ] Trocar a saga coreografada por **orquestrada** e comparar

---

## 12. Convenções

| Item | Convenção |
|---|---|
| Nome de tópico | `<agregado>.<fato no passado>` → `order.created`, `payment.rejected` |
| Versão de evento | Campo `version`. Mudança **compatível** (adicionar campo opcional) mantém versão. Mudança incompatível → nova versão e período com as duas |
| Chave Kafka | Sempre o `orderId` |
| IDs | UUID v7 (ordenável por tempo) ou ULID |
| Datas | ISO-8601 em UTC |
| Dinheiro | Decimal (nunca `float` para somar dinheiro). No JSON, número com 2 casas |
| Erros HTTP | `400` validação · `404` não encontrado · `409` conflito de estado · `422` regra de negócio violada |
| Corpo de erro | `{ "error": "ORDER_NOT_FOUND", "message": "..." }` |
| Commits | Conventional Commits: `feat(order): ...`, `test(catalog): ...`, `fix(payment): ...` |
| Config | Sempre por variável de ambiente (12-factor) |
| Testes manuais | Arquivo `.http` (extensão REST Client do VS Code) |

---

## 13. Referências

**Frameworks e linguagens**
- NestJS — https://docs.nestjs.com
- FastAPI — https://fastapi.tiangolo.com
- Spring Boot — https://docs.spring.io/spring-boot/
- Go — https://go.dev/doc/ · Effective Go — https://go.dev/doc/effective_go

**Mensageria**
- Kafka — https://kafka.apache.org/documentation/
- RabbitMQ — https://www.rabbitmq.com/docs · tutoriais oficiais (há versão em cada linguagem do projeto)

**Kubernetes**
- Kubernetes — https://kubernetes.io/docs/
- kind — https://kind.sigs.k8s.io
- Strimzi — https://strimzi.io/docs/
- RabbitMQ Cluster Operator — https://www.rabbitmq.com/kubernetes/operator/operator-overview

**Testes**
- Testcontainers — https://testcontainers.com (tem módulo para Node, Python, Java e Go)

**Livros / conteúdos**
- *Domain-Driven Design* — Eric Evans · *Implementing DDD* — Vaughn Vernon
- *Clean Architecture* — Robert C. Martin
- *Test-Driven Development: By Example* — Kent Beck
- *Building Microservices* — Sam Newman
- microservices.io (padrões: Saga, Outbox, Database per Service) — Chris Richardson
