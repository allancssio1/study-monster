# ORD — order-service (NestJS / TypeScript)

Bounded context **Ordering**. Cria pedidos, mantém a máquina de estados (`PENDING → CONFIRMED | CANCELLED`) e reage aos eventos de estoque e pagamento. É o primeiro serviço que você constrói e o que mais aparece nas outras fases.

Referências gerais: guia 01 (seções 3, 4, 6, 7.1, 8) e guia 02 (seção 1, TypeScript + NestJS).

---

## ORD-E1 — Setup e fundações (Fase 1)

Projeto Nest rodando, com a estrutura de pastas da seção 7.1 e configuração por variáveis de ambiente.

**Progresso:**
- [ ] ORD-01 Criar o projeto e a estrutura de pastas
- [ ] ORD-02 Configuração por env e endpoint `/health`

### ORD-01 — Criar o projeto e a estrutura de pastas
- **Tipo:** setup · **Fase:** 1 · **Depende de:** INF-01
- **Contexto:** gere o projeto com o CLI do Nest, ligue o `strict` do TypeScript e monte as pastas `shared/` e `modules/orders/{domain,application,infra,presentation}` mais `test/{unit,integration,e2e}`. Decida o runner de testes (Jest padrão do Nest) e o gerenciador de pacotes.
- **Pronto quando:** `npm run start:dev` sobe sem erro, `npm test` roda (pode ter só um teste de exemplo que você apaga depois) e a árvore de pastas bate com a seção 7.1.
- **Referência:** guia 01, seção 7.1; guia 02, seção 1 (arquivo principal e ferramentas).

### ORD-02 — Configuração por env e endpoint `/health`
- **Tipo:** setup · **Fase:** 1 · **Depende de:** ORD-01
- **Contexto:** centralize a leitura de configuração (porta, URL do Postgres, brokers do Kafka, URL do RabbitMQ) num único lugar, com valores padrão para dev e falha clara se faltar algo obrigatório. Adicione `GET /health` fora do módulo de pedidos. Configure o `ValidationPipe` global (`whitelist` e `transform`).
- **Pronto quando:** `GET /health` retorna 200; trocar a porta por variável de ambiente funciona; existe um teste e2e do `/health`.
- **Referência:** guia 01, seções 3.4 e 13 (config 12-factor); guia 02, seção 1.

---

## ORD-E2 — Domínio Ordering (Fase 1)

As regras de negócio puras, sem framework: Value Objects, entidade e aggregate root, tudo guiado por testes.

**Progresso:**
- [ ] ORD-03 Base compartilhada do domínio
- [ ] ORD-04 Value Object `Money`
- [ ] ORD-05 Value Objects `Email` e `OrderStatus`
- [ ] ORD-06 Entity `OrderItem`
- [ ] ORD-07 Aggregate `Order`: criação
- [ ] ORD-08 Aggregate `Order`: confirmar e cancelar

### ORD-03 — Base compartilhada do domínio
- **Tipo:** domain · **Fase:** 1 · **Depende de:** ORD-01
- **Contexto:** em `shared/domain`, crie a classe base de aggregate que acumula domain events e os entrega com `pullEvents()` (devolvendo e esvaziando), o tipo base de evento e o erro base de domínio. Os erros específicos herdarão dele.
- **Pronto quando:** testes unitários cobrem: registrar eventos, `pullEvents()` devolve a lista e limpa, segunda chamada devolve vazio. O código não importa nada do Nest.
- **Referência:** guia 01, seções 4 e 5; guia 02, seção 1 (herança e erros).

### ORD-04 — Value Object `Money`
- **Tipo:** domain · **Fase:** 1 · **Depende de:** ORD-03
- **Contexto:** decida como representar dinheiro sem `float` (centavos inteiros ou lib decimal) e justifique em um comentário curto. O VO é imutável, não aceita valor negativo, tem `add`, `multiply(qty)` e igualdade por valor.
- **Pronto quando:** testes: cria com valor válido; rejeita negativo; `add` devolve um novo objeto e não altera os originais; `multiply` funciona; dois `Money` iguais são iguais; soma como 0,1 + 0,2 não acumula erro de arredondamento.
- **Referência:** guia 01, seções 4, 8 e 13 (dinheiro); guia 02, seção 1 (tipos).

### ORD-05 — Value Objects `Email` e `OrderStatus`
- **Tipo:** domain · **Fase:** 1 · **Depende de:** ORD-03
- **Contexto:** `Email` valida o formato e é imutável. `OrderStatus` é o conjunto `PENDING`, `CONFIRMED`, `CANCELLED` (union de literais ou enum, sua escolha).
- **Pronto quando:** testes: e-mail válido é aceito, inválido lança erro de domínio; status só aceita os três valores; igualdade por valor no `Email`.
- **Referência:** guia 01, seção 4; guia 02, seção 1 (tipos).

### ORD-06 — Entity `OrderItem`
- **Tipo:** domain · **Fase:** 1 · **Depende de:** ORD-04
- **Contexto:** item do pedido com `productId`, `quantity` e `unitPrice` (`Money`). Só é criado e alterado através do `Order`, mas a validação das próprias regras é dele.
- **Pronto quando:** testes: quantidade ≤ 0 é rejeitada; preço unitário ≤ 0 é rejeitado; o subtotal (preço × quantidade) é calculado corretamente.
- **Referência:** guia 01, seções 4 e 8.

### ORD-07 — Aggregate `Order`: criação
- **Tipo:** domain · **Fase:** 1 · **Depende de:** ORD-03, ORD-05, ORD-06
- **Contexto:** `Order` é a raiz do aggregate. Use construtor privado e factory estática para forçar as validações: ao menos 1 item, nasce `PENDING`, total é a soma dos itens, registra `OrderCreated` com os dados que o catalog vai precisar (itens, `customerEmail`, total). Guarde também o motivo de cancelamento (vazio por enquanto).
- **Pronto quando:** testes: cria `PENDING` com total correto; rejeita pedido sem itens; rejeita item inválido; registra `OrderCreated`; `pullEvents()` entrega e limpa.
- **Referência:** guia 01, seções 4 e 8; guia 02, seção 1 (classes e constructor).

### ORD-08 — Aggregate `Order`: confirmar e cancelar
- **Tipo:** domain · **Fase:** 1 · **Depende de:** ORD-07
- **Contexto:** implemente a máquina de estados. `confirm()` só a partir de `PENDING` e registra `OrderConfirmed`. `cancel(reason)` só a partir de `PENDING`, guarda o motivo e registra `OrderCancelled`. Transição inválida lança erro de domínio.
- **Pronto quando:** testes: `confirm()` muda para `CONFIRMED`; `confirm()` em `CANCELLED` lança erro; `cancel(reason)` muda para `CANCELLED` e guarda o motivo; `cancel()` em `CONFIRMED` lança erro; os eventos certos são registrados em cada caminho.
- **Referência:** guia 01, seções 2.3 e 8.

---

## ORD-E3 — Casos de uso (Fase 1 e 3)

Orquestração da camada de aplicação com repositório em memória e publishers falsos. Nenhum use case conhece HTTP, Kafka ou SQL.

**Progresso:**
- [ ] ORD-09 Ports e fakes
- [ ] ORD-10 `CreateOrder`
- [ ] ORD-11 `GetOrder`
- [ ] ORD-12 `ConfirmOrder`
- [ ] ORD-13 `CancelOrder`
- [ ] ORD-14 Idempotência por `eventId`

### ORD-09 — Ports e fakes
- **Tipo:** use-case · **Fase:** 1 · **Depende de:** ORD-07
- **Contexto:** defina os ports da aplicação: `OrderRepository` (no domínio), `EventPublisher` e `NotificationSender`. Como interfaces somem em runtime no Nest, use `abstract class` ou tokens. Crie o `InMemoryOrderRepository` (fake de verdade, que o resto dos testes vai reutilizar) e um publisher e sender falsos que guardam o que receberam.
- **Pronto quando:** o repositório em memória salva e recupera um `Order`; os fakes expõem o que foi publicado para as asserções; nada na camada `application` importa `@nestjs`.
- **Referência:** guia 01, seções 5 e 6; guia 02, seção 1 (interfaces e injeção de dependência).

### ORD-10 — Caso de uso `CreateOrder`
- **Tipo:** use-case · **Fase:** 1 · **Depende de:** ORD-09
- **Contexto:** recebe o input (cliente, e-mail, itens com preço), cria o `Order`, salva e publica os eventos puxados do aggregate. Defina os DTOs de entrada e saída (`id`, `status`, `total`).
- **Pronto quando:** testes: salva no repositório e publica `order.created`; não publica nada (nem salva) se o domínio rejeitar; a saída traz id, status `PENDING` e total.
- **Referência:** guia 01, seções 6 (fluxo de um request) e 8.

### ORD-11 — Caso de uso `GetOrder`
- **Tipo:** use-case · **Fase:** 1 · **Depende de:** ORD-09
- **Contexto:** busca por id e devolve o DTO de saída (com `cancelReason` quando houver). Crie o erro `OrderNotFound`.
- **Pronto quando:** testes: devolve o pedido existente; pedido inexistente lança `OrderNotFound`.
- **Referência:** guia 01, seções 3.4 e 8.

### ORD-12 — Caso de uso `ConfirmOrder`
- **Tipo:** use-case · **Fase:** 3 · **Depende de:** ORD-08, ORD-09
- **Contexto:** carrega o pedido, chama `confirm()`, salva, publica `order.confirmed` e envia o comando de e-mail `order-confirmed`. **O payload de `order.confirmed` leva `orderId` e `customerEmail`** (o shipping depende disso).
- **Pronto quando:** testes: confirma, publica `order.confirmed` com `customerEmail` e envia o e-mail pelo sender; pedido inexistente lança `OrderNotFound`; pedido já cancelado propaga o erro de domínio e não publica nada.
- **Referência:** guia 01, seções 3.2, 3.3 e 8.

### ORD-13 — Caso de uso `CancelOrder`
- **Tipo:** use-case · **Fase:** 3 · **Depende de:** ORD-08, ORD-09
- **Contexto:** carrega o pedido, chama `cancel(reason)` (motivos: sem estoque ou pagamento recusado), salva, publica `order.cancelled` e envia o e-mail `order-cancelled`.
- **Pronto quando:** testes: cancela com motivo, publica `order.cancelled` e envia o e-mail; pedido inexistente lança `OrderNotFound`; pedido já confirmado propaga erro e não publica.
- **Referência:** guia 01, seções 2.2 e 8.

### ORD-14 — Idempotência por `eventId`
- **Tipo:** use-case · **Fase:** 3 · **Depende de:** ORD-12, ORD-13
- **Contexto:** Kafka entrega pelo menos uma vez, então processar o mesmo evento duas vezes não pode mudar nada. Crie um port de "eventos já processados" (com versão em memória) e aplique-o nos casos de uso que reagem a eventos. Decida onde a checagem mora (use case ou um decorator) e anote o porquê.
- **Pronto quando:** testes: o mesmo `eventId` aplicado duas vezes a `ConfirmOrder` e a `CancelOrder` produz um único efeito (uma publicação, um e-mail); `eventId`s diferentes são processados normalmente.
- **Referência:** guia 01, seções 3.1, 6 (fluxo de uma mensagem) e 8.

---

## ORD-E4 — API HTTP (Fase 1)

Controller, validação de entrada, tratamento de erros e testes e2e, ainda com repositório em memória.

**Progresso:**
- [ ] ORD-15 `POST /orders`
- [ ] ORD-16 `GET /orders/:id`
- [ ] ORD-17 Exception filter
- [ ] ORD-18 Testes e2e de `POST /orders`
- [ ] ORD-19 Testes e2e de `GET /orders/:id`

### ORD-15 — `POST /orders`
- **Tipo:** api · **Fase:** 1 · **Depende de:** ORD-10, ORD-02
- **Contexto:** crie o `OrdersModule` ligando os ports aos fakes/in-memory (por enquanto), o controller e o DTO de request com `class-validator` (cliente, e-mail, lista de itens aninhada com quantidade inteira ≥ 1 e preço positivo). Responde 201 com `{ id, status, total }`.
- **Pronto quando:** uma chamada manual com body válido devolve 201 com status `PENDING`; body inválido devolve 400 (a formatação do erro vem na ORD-17).
- **Referência:** guia 01, seção 3.4; guia 02, seção 1 (decorators do Nest e DTO).

### ORD-16 — `GET /orders/:id`
- **Tipo:** api · **Fase:** 1 · **Depende de:** ORD-11, ORD-15
- **Contexto:** rota que chama `GetOrder` e devolve `{ id, status, items, total, cancelReason? }`.
- **Pronto quando:** uma chamada manual para um pedido criado devolve 200 com os campos esperados.
- **Referência:** guia 01, seção 3.4.

### ORD-17 — Exception filter
- **Tipo:** api · **Fase:** 1 · **Depende de:** ORD-15
- **Contexto:** traduza erros de domínio para HTTP com corpo padronizado `{ error, message }`: não encontrado → 404, transição inválida → 409, regra de negócio violada → 422, validação de formato → 400. Mantenha o mapeamento num lugar só.
- **Pronto quando:** teste unitário do filter cobre cada tipo de erro → status e código; o formato do corpo segue a seção 13 do guia.
- **Referência:** guia 01, seção 13 (erros HTTP); guia 02, seção 1 (erros e exception filter).

### ORD-18 — Testes e2e de `POST /orders`
- **Tipo:** test-e2e · **Fase:** 1 · **Depende de:** ORD-17
- **Contexto:** suba o app com `Test.createTestingModule`, sobrescreva os providers de publisher e sender pelos fakes e use `supertest`.
- **Pronto quando:** testes: body válido → 201 e corpo com `id`, `status: PENDING`, `total` correto; sem itens → 400; quantidade zero → 400; e-mail inválido → 400; o publisher fake recebeu `order.created`.
- **Referência:** guia 01, seção 8 (Integration / E2E); guia 02, seção 1 (testes).

### ORD-19 — Testes e2e de `GET /orders/:id`
- **Tipo:** test-e2e · **Fase:** 1 · **Depende de:** ORD-16, ORD-17
- **Contexto:** mesmo setup do e2e anterior. Crie um pedido via POST e leia via GET.
- **Pronto quando:** testes: pedido existente → 200 com itens e total; id inexistente → 404 com `{ error: "ORDER_NOT_FOUND", ... }`.
- **Referência:** guia 01, seção 8.

---

## ORD-E5 — Persistência em Postgres (Fase 1 e 3)

Trocar o repositório em memória por um real, mantendo o domínio intacto.

**Progresso:**
- [ ] ORD-20 ORM e migrations
- [ ] ORD-21 `PostgresOrderRepository`
- [ ] ORD-22 Teste de integração do repositório
- [ ] ORD-23 Ligar o Postgres no módulo e rodar o e2e
- [ ] ORD-24 Persistência de eventos processados

### ORD-20 — ORM e migrations
- **Tipo:** persistence · **Fase:** 1 · **Depende de:** ORD-01, INF-02
- **Contexto:** escolha a ferramenta (Prisma, TypeORM ou Drizzle) e anote o motivo. Modele as tabelas de pedidos e de itens e a de eventos processados, e versione as migrations. As tabelas são modelo de persistência, **não** as entidades de domínio.
- **Pronto quando:** rodar as migrations contra a database `orders` do compose cria as 3 tabelas; um comando documentado recria o schema do zero.
- **Referência:** guia 01, seções 6 e 7.1 (infra/database).

### ORD-21 — `PostgresOrderRepository`
- **Tipo:** persistence · **Fase:** 1 · **Depende de:** ORD-20, ORD-09
- **Contexto:** implemente o port `OrderRepository` convertendo entre o modelo de tabela e o aggregate (reconstituição sem passar pelas validações de criação, mas respeitando o estado salvo). `save` deve gravar pedido e itens na mesma transação.
- **Pronto quando:** o repositório compila contra o port; salvar e recarregar devolve um `Order` equivalente (verificado na ORD-22).
- **Referência:** guia 01, seções 5 (Repository) e 6.

### ORD-22 — Teste de integração do repositório
- **Tipo:** test-integration · **Fase:** 1 · **Depende de:** ORD-21
- **Contexto:** use Testcontainers com Postgres real, aplique as migrations e teste o repositório de verdade. Rode também os mesmos cenários contra o repositório em memória para provar a substituição (Liskov).
- **Pronto quando:** testes: salva e recupera um `Order` com vários itens; salvar de novo (após confirmar) atualiza em vez de duplicar; id inexistente devolve nulo; mesma suíte de comportamento passa nos dois repositórios.
- **Referência:** guia 01, seção 8; guia 02, seção 1 (testes).

### ORD-23 — Ligar o Postgres no módulo e rodar o e2e
- **Tipo:** persistence · **Fase:** 1 · **Depende de:** ORD-21, ORD-19
- **Contexto:** troque o provider do `OrderRepository` no `OrdersModule` pelo Postgres. Os e2e passam a rodar contra banco real (container de teste ou o do compose). O publisher continua fake.
- **Pronto quando:** os e2e de POST e GET passam contra Postgres real. **Critério de pronto da fase 1:** `POST /orders` e `GET /orders/:id` funcionam, o publisher ainda é fake e todos os testes estão verdes.
- **Referência:** guia 01, seção 12 (fase 1).

### ORD-24 — Persistência de eventos processados
- **Tipo:** persistence · **Fase:** 3 · **Depende de:** ORD-14, ORD-20
- **Contexto:** implemente o port de eventos processados da ORD-14 sobre a tabela criada na ORD-20. Pense em concorrência: duas mensagens iguais chegando ao mesmo tempo não podem passar as duas (restrição única + tratamento do conflito).
- **Pronto quando:** teste de integração: registrar o mesmo `eventId` duas vezes é detectado; duas tentativas simultâneas resultam em um único processamento.
- **Referência:** guia 01, seção 3.1 (idempotência).

---

## ORD-E6 — Mensageria Kafka (Fase 2 e 3)

Publicar os eventos do order e consumir os de estoque e pagamento.

**Progresso:**
- [ ] ORD-25 `KafkaEventPublisher`
- [ ] ORD-26 Integração do publisher
- [ ] ORD-27 Trocar o publisher fake pelo Kafka
- [ ] ORD-28 Consumer de `stock.failed`
- [ ] ORD-29 Consumers de `payment.approved` e `payment.rejected`
- [ ] ORD-30 Integração dos consumers

### ORD-25 — `KafkaEventPublisher`
- **Tipo:** messaging · **Fase:** 2 · **Depende de:** ORD-09, INF-06
- **Contexto:** implemente o port `EventPublisher` serializando cada domain event no envelope padrão (`eventId`, `eventType`, `version`, `occurredAt`, `aggregateId`, `payload`), usando `orderId` como chave da mensagem e o tipo do evento como tópico. Só a infra muda; o domínio e os casos de uso ficam como estão.
- **Pronto quando:** publicar um `OrderCreated` produz no tópico `order.created` uma mensagem com envelope completo e chave `orderId` (visível na Kafka UI).
- **Referência:** guia 01, seções 3.1, 3.2 e 12 (fase 2); guia 02, seção 1 (Kafka).

### ORD-26 — Integração do publisher
- **Tipo:** test-integration · **Fase:** 2 · **Depende de:** ORD-25
- **Contexto:** Testcontainers com Kafka. Leia de volta o que foi publicado.
- **Pronto quando:** testes: envelope tem todos os campos e `occurredAt` em ISO-8601 UTC; a chave é o `orderId`; dois eventos do mesmo pedido caem na mesma partição.
- **Referência:** guia 01, seções 3.1 e 8.

### ORD-27 — Trocar o publisher fake pelo Kafka
- **Tipo:** messaging · **Fase:** 2 · **Depende de:** ORD-25, ORD-23
- **Contexto:** troque o provider no módulo. Rode o app com o compose e crie um pedido. Os consumers do catalog ainda não existem, então só confira a publicação.
- **Pronto quando:** `POST /orders` com o compose no ar cria o pedido no Postgres **e** aparece uma mensagem em `order.created` na Kafka UI.
- **Referência:** guia 01, seção 12 (fase 2).

### ORD-28 — Consumer de `stock.failed`
- **Tipo:** messaging · **Fase:** 3 · **Depende de:** ORD-13, ORD-14, ORD-27
- **Contexto:** controller de mensageria que desserializa a mensagem, checa o `eventId`, chama `CancelOrder` com o motivo "sem estoque" e só então confirma o offset. Consumer group `order-service`. A camada de presentation não tem regra de negócio.
- **Pronto quando:** publicar manualmente um `stock.failed` (pela Kafka UI) para um pedido `PENDING` o leva a `CANCELLED`; reenviar a mesma mensagem não muda nada.
- **Referência:** guia 01, seção 6 (fluxo de uma mensagem); guia 02, seção 1 (decorators `@EventPattern`).

### ORD-29 — Consumers de `payment.approved` e `payment.rejected`
- **Tipo:** messaging · **Fase:** 3 · **Depende de:** ORD-12, ORD-13, ORD-14, ORD-27
- **Contexto:** `payment.approved` chama `ConfirmOrder`; `payment.rejected` chama `CancelOrder` com o motivo "pagamento recusado". Mesma estrutura do consumer anterior.
- **Pronto quando:** publicar manualmente cada evento leva o pedido a `CONFIRMED` ou `CANCELLED`; ambos aparecem publicando `order.confirmed` (com `customerEmail`) e `order.cancelled`; duplicatas são ignoradas.
- **Referência:** guia 01, seções 2.2 e 3.2.

### ORD-30 — Integração dos consumers
- **Tipo:** test-integration · **Fase:** 3 · **Depende de:** ORD-28, ORD-29, ORD-24
- **Contexto:** teste com Kafka e Postgres reais: publique eventos de entrada e verifique o estado do pedido e o que foi publicado.
- **Pronto quando:** testes: aprovação confirma; rejeição cancela com motivo; falta de estoque cancela; evento duplicado é ignorado; evento para pedido inexistente não derruba o consumer; mensagem malformada é tratada sem travar a partição.
- **Referência:** guia 01, seção 8.

---

## ORD-E7 — Mensageria RabbitMQ (Fase 4)

Enviar o comando de e-mail para o notification-service.

**Progresso:**
- [ ] ORD-31 `RabbitMqNotificationSender`
- [ ] ORD-32 Integração com RabbitMQ
- [ ] ORD-33 Ligar o sender e validar com o notification

### ORD-31 — `RabbitMqNotificationSender`
- **Tipo:** messaging · **Fase:** 4 · **Depende de:** ORD-09, INF-05
- **Contexto:** implemente o port `NotificationSender` com `amqplib` direto (não o transporte RMQ do Nest, que embrulha a mensagem). Canal com confirmação, exchange `notifications` durável, routing key `email.send`, mensagem persistente com o formato da seção 3.3 (`messageId`, `to`, `template`, `data`).
- **Pronto quando:** enviar um comando faz a mensagem aparecer na fila `notifications.email` (visível no painel) com JSON exatamente no contrato.
- **Referência:** guia 01, seção 3.3; guia 02, seção 1 (RabbitMQ).

### ORD-32 — Integração com RabbitMQ
- **Tipo:** test-integration · **Fase:** 4 · **Depende de:** ORD-31
- **Contexto:** Testcontainers com RabbitMQ; consuma a fila no teste e valide o conteúdo.
- **Pronto quando:** testes: a mensagem chega com `template` e `data` corretos e `messageId` único por envio; é persistente; falha de conexão resulta em erro propagado (e não silêncio).
- **Referência:** guia 01, seção 8.

### ORD-33 — Ligar o sender e validar com o notification
- **Tipo:** messaging · **Fase:** 4 · **Depende de:** ORD-31, ORD-29
- **Contexto:** troque o sender fake no módulo. Confirmar ou cancelar um pedido deve produzir o comando de e-mail com `orderId` e `total` em `data`.
- **Pronto quando:** com o compose no ar, fazer um pedido ser confirmado coloca uma mensagem `order-confirmed` na fila; um cancelado coloca `order-cancelled`.
- **Referência:** guia 01, seções 3.3 e 12 (fase 4).

---

## ORD-E8 — Container (Fase 6)

**Progresso:**
- [ ] ORD-34 Dockerfile multi-stage

### ORD-34 — Dockerfile multi-stage
- **Tipo:** infra · **Fase:** 6 · **Depende de:** ORD-02
- **Contexto:** estágio de build com dependências completas e estágio final só com o necessário para rodar (build compilado, dependências de produção, usuário sem privilégio). Sem segredos na imagem; configuração só por env. Defina um `HEALTHCHECK` ou deixe para o compose.
- **Pronto quando:** `docker build` gera a imagem, e ela sobe e responde `/health`; o tamanho final é bem menor que o do estágio de build; o `.dockerignore` evita copiar `node_modules`.
- **Referência:** guia 01, seção 12 (fase 6).
