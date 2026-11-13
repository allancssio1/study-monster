# SHP — shipping-service (PHP / Laravel)

Bounded context **Shipping**. Consome `order.confirmed` e cria um `Shipment`; a API despacha e entrega; o despacho publica `shipment.dispatched` no Kafka e pede o e-mail `order-shipped` ao RabbitMQ. Fica **depois** da saga e não participa da compensação. Fase 5.

Referências gerais: guia 01 (seções 3, 4, 7.5, 8) e guia 02 (seção 5, PHP + Laravel).

Pontos de atenção deste serviço:
- Laravel usa Active Record (Eloquent). Para manter o domínio limpo, o model Eloquent fica só na infra e o repositório converte para a entidade.
- PHP é um processo por request; o consumer Kafka é um **comando Artisan** de longa duração, não parte da API.
- O driver de filas do Laravel **não** serve para falar com o consumer Go (formato próprio). Use `php-amqplib` direto.

---

## SHP-E1 — Setup (Fase 5)

**Progresso:**
- [ ] SHP-01 Criar o projeto e a estrutura de módulos
- [ ] SHP-02 Rotas de API, `/health` e configuração

### SHP-01 — Criar o projeto e a estrutura de módulos
- **Tipo:** setup · **Fase:** 5 · **Depende de:** INF-01
- **Contexto:** crie o projeto com Composer, habilite as rotas de API, instale Pest e Pint e monte `app/Modules/Shipping/{Domain,Application,Infra,Presentation}` e `app/Shared/Domain` conforme a seção 7.5. Garanta que o autoload PSR-4 cobre o novo namespace e que `declare(strict_types=1)` é seu padrão. O serviço é autocontido: crie dentro da pasta dele um `.gitignore` e um `.dockerignore` próprios, um `.env.example` com todas as variáveis e um `README.md` curto (como rodar, testar e buildar), sem depender de arquivos da raiz.
- **Pronto quando:** `php artisan serve --port=8082` sobe, `./vendor/bin/pest` roda (com um teste de exemplo que você pode apagar) e `./vendor/bin/pint --test` passa; a árvore bate com a seção 7.5. A pasta do serviço, copiada sozinha para outro lugar, roda, testa e builda sem arquivos de fora dela; existem `.gitignore`, `.dockerignore`, `.env.example` e `README.md` na pasta.
- **Referência:** guia 01, seção 7.5; guia 02, seção 5 (ferramentas e módulos).

### SHP-02 — Rotas de API, `/health` e configuração
- **Tipo:** setup · **Fase:** 5 · **Depende de:** SHP-01
- **Contexto:** no `bootstrap/app.php`, remova o prefixo `/api` das rotas e exponha `/health`. Crie `config/shipping.php` lendo do ambiente: brokers do Kafka, nomes de tópicos e group id, URL do RabbitMQ. Nada de `env()` fora dos arquivos de config.
- **Pronto quando:** `GET /health` responde 200; um teste de feature confirma; o valor de config muda ao alterar o `.env`.
- **Referência:** guia 01, seção 3.4; guia 02, seção 5 (arquivo principal).

---

## SHP-E2 — Domínio Shipping (Fase 5)

**Progresso:**
- [ ] SHP-03 Base compartilhada, `ShipmentStatus` e `TrackingCode`
- [ ] SHP-04 Aggregate `Shipment`: criação
- [ ] SHP-05 Aggregate `Shipment`: despachar e entregar

### SHP-03 — Base compartilhada, `ShipmentStatus` e `TrackingCode`
- **Tipo:** domain · **Fase:** 5 · **Depende de:** SHP-01
- **Contexto:** em `Shared/Domain`, a base de aggregate (acumula e entrega eventos), o evento base e o erro de domínio. Depois o enum `ShipmentStatus` (`PENDING`, `DISPATCHED`, `DELIVERED`) e o VO `TrackingCode` (classe `readonly`, formato `SF` + 10 caracteres, geração aleatória, igualdade por valor).
- **Pronto quando:** testes (Pest): `pullEvents` entrega e limpa; formato inválido é rejeitado (dataset com vários casos); dois códigos com o mesmo valor são iguais; o código gerado sempre respeita o formato; enum rejeita valor desconhecido em `from`.
- **Referência:** guia 01, seção 4; guia 02, seção 5 (tipos, classes e testes).

### SHP-04 — Aggregate `Shipment`: criação
- **Tipo:** domain · **Fase:** 5 · **Depende de:** SHP-03
- **Contexto:** construtor privado e fábrica estática: um `Shipment` nasce `PENDING`, guarda `orderId` e `customerEmail` e registra `ShipmentCreated`. Prepare também uma fábrica de reconstituição para o repositório.
- **Pronto quando:** testes: nasce `PENDING` sem código de rastreio; evento `ShipmentCreated` registrado; o e-mail do cliente é guardado; propriedades `readonly` não podem ser reatribuídas.
- **Referência:** guia 01, seções 4 e 8; guia 02, seção 5 (classes e constructor promotion).

### SHP-05 — Aggregate `Shipment`: despachar e entregar
- **Tipo:** domain · **Fase:** 5 · **Depende de:** SHP-04
- **Contexto:** `dispatch()` só a partir de `PENDING`, gera o `TrackingCode` e registra `ShipmentDispatched`. `deliver()` só a partir de `DISPATCHED` e registra `ShipmentDelivered`. O status só avança.
- **Pronto quando:** testes: `dispatch()` muda o status, gera código e registra o evento; `dispatch()` fora de `PENDING` lança erro; `deliver()` fora de `DISPATCHED` lança erro; `deliver()` muda para `DELIVERED`.
- **Referência:** guia 01, seção 8.

---

## SHP-E3 — Casos de uso (Fase 5)

**Progresso:**
- [ ] SHP-06 Ports, repositório em memória e fakes
- [ ] SHP-07 `CreateShipment`
- [ ] SHP-08 `DispatchShipment`
- [ ] SHP-09 `DeliverShipment` e `GetShipment`
- [ ] SHP-10 `ShippingServiceProvider`
- [ ] SHP-11 Idempotência por `eventId`

### SHP-06 — Ports, repositório em memória e fakes
- **Tipo:** use-case · **Fase:** 5 · **Depende de:** SHP-05
- **Contexto:** interfaces `ShipmentRepository` (domínio), `EventPublisher` e `NotificationSender` (application), um `InMemoryShipmentRepository` e fakes de publisher e sender que guardam o que receberam. Use cases são PHP puro, sem facades.
- **Pronto quando:** o repositório em memória salva, busca por id e por `orderId`; os fakes expõem o que foi publicado; nada em `Application` usa classes do Laravel.
- **Referência:** guia 01, seções 4 e 6; guia 02, seção 5 (interfaces).

### SHP-07 — `CreateShipment`
- **Tipo:** use-case · **Fase:** 5 · **Depende de:** SHP-06
- **Contexto:** disparado pelo `order.confirmed` (`orderId` e `customerEmail`). Cria e salva o `Shipment`. Um por `orderId`: repetir não cria outro.
- **Pronto quando:** testes: cria `PENDING` e salva; segunda chamada com o mesmo `orderId` não cria outro e não falha; `customerEmail` ausente é rejeitado com erro claro.
- **Referência:** guia 01, seção 8.

### SHP-08 — `DispatchShipment`
- **Tipo:** use-case · **Fase:** 5 · **Depende de:** SHP-06
- **Contexto:** carrega pelo id, chama `dispatch()`, salva, publica `shipment.dispatched` (`orderId`, `shipmentId`, `trackingCode`) e envia o comando de e-mail `order-shipped` para o `customerEmail`. Crie `ShipmentNotFound`.
- **Pronto quando:** testes: despacha, publica o evento e envia o e-mail com `trackingCode` em `data`; id inexistente lança `ShipmentNotFound`; despachar duas vezes propaga o erro de transição e não publica de novo.
- **Referência:** guia 01, seções 3.2, 3.3 e 8.

### SHP-09 — `DeliverShipment` e `GetShipment`
- **Tipo:** use-case · **Fase:** 5 · **Depende de:** SHP-06
- **Contexto:** `DeliverShipment` marca a entrega. `GetShipment` busca por `orderId` e devolve um DTO (`id`, `orderId`, `status`, `trackingCode` opcional).
- **Pronto quando:** testes: entrega após o despacho; entrega antes do despacho propaga erro; busca por `orderId` existente devolve o DTO e inexistente lança `ShipmentNotFound`.
- **Referência:** guia 01, seção 3.4.

### SHP-10 — `ShippingServiceProvider`
- **Tipo:** use-case · **Fase:** 5 · **Depende de:** SHP-07, SHP-08, SHP-09, SHP-02
- **Contexto:** registre o provider (em `bootstrap/providers.php`) ligando cada port à implementação via container. Os use cases não precisam de registro: o container os resolve pelo type-hint do construtor. Por enquanto, repositório em memória e fakes.
- **Pronto quando:** um teste resolve cada use case pelo container e confirma que as dependências injetadas são as esperadas; trocar um binding num teste (`app->bind`) funciona.
- **Referência:** guia 02, seção 5 (injeção de dependência).

### SHP-11 — Idempotência por `eventId`
- **Tipo:** use-case · **Fase:** 5 · **Depende de:** SHP-07
- **Contexto:** port de eventos processados (versão em memória). O consumer do Kafka chama o `CreateShipment` com o `eventId`; o mesmo evento chegando duas vezes não pode gerar efeito, nem mesmo se o `orderId` fosse diferente por engano.
- **Pronto quando:** testes: o mesmo `eventId` só tem efeito uma vez; `eventId`s diferentes seguem normalmente.
- **Referência:** guia 01, seção 3.1.

---

## SHP-E4 — API HTTP (Fase 5)

**Progresso:**
- [ ] SHP-12 Rotas e controller
- [ ] SHP-13 Mapeamento de erros
- [ ] SHP-14 Testes de feature da API

### SHP-12 — Rotas e controller
- **Tipo:** api · **Fase:** 5 · **Depende de:** SHP-10
- **Contexto:** `GET /shipments?orderId=`, `PATCH /shipments/{id}/dispatch` e `PATCH /shipments/{id}/deliver` em `routes/api.php`, apontando para um controller fino que injeta o use case no método e devolve JSON. Valide o parâmetro `orderId`.
- **Pronto quando:** com um `Shipment` criado pela via interna (um teste ou seeder), as três rotas funcionam manualmente; `orderId` ausente devolve 422 do Laravel (ou 400, se você mapear).
- **Referência:** guia 01, seção 3.4; guia 02, seção 5 (decorators/atributos e controller).

### SHP-13 — Mapeamento de erros
- **Tipo:** api · **Fase:** 5 · **Depende de:** SHP-12
- **Contexto:** no `bootstrap/app.php`, mapeie `ShipmentNotFound` para 404 (`SHIPMENT_NOT_FOUND`) e transição inválida para 409, com corpo `{ error, message }`. Desative a página/JSON padrão do Laravel para esses casos.
- **Pronto quando:** o formato do erro segue a seção 13; despachar um `Shipment` já despachado devolve 409.
- **Referência:** guia 01, seção 13; guia 02, seção 5 (erros).

### SHP-14 — Testes de feature da API
- **Tipo:** test-e2e · **Fase:** 5 · **Depende de:** SHP-13
- **Contexto:** testes de feature do Laravel/Pest (`getJson`, `patchJson`) com repositório em memória e fakes via `app->bind`.
- **Pronto quando:** testes: `GET` por `orderId` → 200 e 404; `PATCH dispatch` → 200 com `trackingCode`, 404 para id inexistente e 409 se não estiver `PENDING`; `PATCH deliver` → 200, 404 e 409 se não estiver `DISPATCHED`; o fake de publisher recebeu `shipment.dispatched`.
- **Referência:** guia 01, seção 8; guia 02, seção 5 (testes).

---

## SHP-E5 — Persistência em Postgres (Fase 5)

**Progresso:**
- [ ] SHP-15 Migrations e `ShipmentModel`
- [ ] SHP-16 `EloquentShipmentRepository`
- [ ] SHP-17 Teste de integração do repositório
- [ ] SHP-18 Ligar o Eloquent e rodar os testes de feature

### SHP-15 — Migrations e `ShipmentModel`
- **Tipo:** persistence · **Fase:** 5 · **Depende de:** SHP-01, INF-02
- **Contexto:** migrations para `shipments` (id string, `order_id` único, e-mail, status, código de rastreio nulo, timestamps) e `processed_events`. O `ShipmentModel` (Eloquent) mora em `Infra/Database` e **não** é a entidade de domínio. Aponte a conexão para a database `shipping`.
- **Pronto quando:** `php artisan migrate` cria as tabelas; `migrate:fresh` recria do zero; a restrição única em `order_id` existe.
- **Referência:** guia 01, seção 7.5; guia 02, seção 5 (Eloquent).

### SHP-16 — `EloquentShipmentRepository`
- **Tipo:** persistence · **Fase:** 5 · **Depende de:** SHP-15, SHP-06, SHP-11
- **Contexto:** implemente o repositório convertendo model ↔ entidade (reconstituição sem revalidar as regras de criação) e o repositório de eventos processados. Trate a violação da restrição única como "já existe", não como falha genérica.
- **Pronto quando:** o adapter satisfaz a interface do domínio; nenhuma classe de `Domain` ou `Application` referencia Eloquent.
- **Referência:** guia 01, seção 7.5 (nota sobre Active Record); guia 02, seção 5.

### SHP-17 — Teste de integração do repositório
- **Tipo:** test-integration · **Fase:** 5 · **Depende de:** SHP-16
- **Contexto:** teste contra Postgres real (o do compose ou Testcontainers), com `RefreshDatabase`. Rode a mesma suíte de comportamento no repositório em memória e no Eloquent.
- **Pronto quando:** testes: salva e recupera preservando status, código e e-mail; busca por `orderId`; duplicidade de `orderId` é tratada; evento processado repetido é detectado.
- **Referência:** guia 01, seção 8; guia 02, seção 5 (testes).

### SHP-18 — Ligar o Eloquent e rodar os testes de feature
- **Tipo:** persistence · **Fase:** 5 · **Depende de:** SHP-16, SHP-14
- **Contexto:** troque o binding do repositório no provider para o Eloquent e reexecute os testes de feature da API (agora com banco real e `RefreshDatabase`).
- **Pronto quando:** os testes da SHP-14 passam contra Postgres; os dados sobrevivem a reiniciar o servidor.
- **Referência:** guia 01, seção 12 (fase 5).

---

## SHP-E6 — Mensageria Kafka (Fase 5)

**Progresso:**
- [ ] SHP-19 Ambiente PHP com `ext-rdkafka`
- [ ] SHP-20 `KafkaEventPublisher`
- [ ] SHP-21 Consumer e comando `shipping:consume-orders`
- [ ] SHP-22 Integração com Kafka

### SHP-19 — Ambiente PHP com `ext-rdkafka`
- **Tipo:** setup · **Fase:** 5 · **Depende de:** SHP-01, INF-03
- **Contexto:** o pacote `laravel-kafka` exige a extensão `rdkafka` (e a `librdkafka`). Instale no seu ambiente local (ou já num container de desenvolvimento) e adicione o pacote. Faça uma publicação e uma leitura de teste para provar a conexão com `localhost:9092`. Confira os nomes dos métodos na versão instalada: a API do pacote mudou entre versões.
- **Pronto quando:** `php -m` lista `rdkafka`; um script ou comando de teste publica uma mensagem e outro a lê; documente o passo de instalação no README do serviço.
- **Referência:** guia 01, seção 7.5; guia 02, seção 5 (Kafka e RabbitMQ).

### SHP-20 — `KafkaEventPublisher`
- **Tipo:** messaging · **Fase:** 5 · **Depende de:** SHP-19, SHP-06
- **Contexto:** implemente o port publicando `shipment.dispatched` no envelope padrão (`eventId`, `eventType`, `version`, `occurredAt`, `aggregateId`, `payload`) com chave `orderId`. A serialização do envelope é da infra.
- **Pronto quando:** despachar um shipment pela API deixa uma mensagem com envelope completo e chave `orderId` em `shipment.dispatched` (Kafka UI).
- **Referência:** guia 01, seções 3.1 e 3.2.

### SHP-21 — Consumer e comando `shipping:consume-orders`
- **Tipo:** messaging · **Fase:** 5 · **Depende de:** SHP-19, SHP-11, SHP-18, ORD-12
- **Contexto:** `OrderEventsConsumer` na camada de presentation, chamado por um comando Artisan de longa duração: group `shipping-service`, tópico `order.confirmed`, commit manual depois do processamento. Traduza o envelope externo para o input do `CreateShipment` (camada anticorrupção). Trate `SIGTERM` para parar com elegância e decida o que fazer com mensagem malformada.
- **Pronto quando:** confirmar um pedido no order faz o worker criar um `Shipment` `PENDING` com o e-mail do cliente; repetir a mensagem não cria outro; parar o worker com `Ctrl+C` termina a mensagem atual antes de sair; reiniciar não reprocessa o que já foi confirmado.
- **Referência:** guia 01, seções 6 e 7.5; guia 02, seção 5 (async e Kafka).

### SHP-22 — Integração com Kafka
- **Tipo:** test-integration · **Fase:** 5 · **Depende de:** SHP-20, SHP-21
- **Contexto:** teste com Kafka e Postgres reais: publique `order.confirmed` e verifique o banco; despache e consuma `shipment.dispatched`.
- **Pronto quando:** testes: evento cria o `Shipment`; evento duplicado não duplica; `customerEmail` do payload é guardado; o despacho publica o evento esperado com chave `orderId`.
- **Referência:** guia 01, seção 8.

---

## SHP-E7 — Mensageria RabbitMQ (Fase 5)

**Progresso:**
- [ ] SHP-23 `RabbitMqNotificationSender`
- [ ] SHP-24 Integração com RabbitMQ
- [ ] SHP-25 Validar a fase 5

### SHP-23 — `RabbitMqNotificationSender`
- **Tipo:** messaging · **Fase:** 5 · **Depende de:** SHP-06, INF-05
- **Contexto:** use `php-amqplib` direto (não o driver de filas do Laravel). Exchange `notifications` durável, routing key `email.send`, mensagem persistente e canal com confirmação, no formato da seção 3.3 com template `order-shipped` e `data` com `orderId` e `trackingCode`. Feche a conexão corretamente num processo curto como o request HTTP.
- **Pronto quando:** despachar um shipment coloca uma mensagem `order-shipped` na fila `notifications.email`, com JSON exatamente no contrato.
- **Referência:** guia 01, seção 3.3; guia 02, seção 5 (RabbitMQ).

### SHP-24 — Integração com RabbitMQ
- **Tipo:** test-integration · **Fase:** 5 · **Depende de:** SHP-23
- **Contexto:** RabbitMQ real; leia a fila no teste.
- **Pronto quando:** testes: a mensagem chega com `template`, `to` e `data` corretos e `messageId` único; é persistente; falha de conexão propaga erro.
- **Referência:** guia 01, seção 8.

### SHP-25 — Validar a fase 5
- **Tipo:** exercise · **Fase:** 5 · **Depende de:** SHP-21, SHP-23, SHP-20, NOT-13
- **Contexto:** com order, catalog, payment, notification e shipping no ar, confirme um pedido e despache o shipment pela API. O notification já aceita o template `order-shipped` (NOT-04).
- **Pronto quando:** **critério de pronto da fase 5:** um pedido confirmado gera um `Shipment`; o despacho publica `shipment.dispatched` e o e-mail `order-shipped` aparece no log do notification; `shipment.dispatched` permanece sem consumidor (fica disponível para exercícios de replay).
- **Referência:** guia 01, seção 12 (fase 5).

---

## SHP-E8 — Container (Fase 6)

**Progresso:**
- [ ] SHP-26 Dockerfile multi-stage

### SHP-26 — Dockerfile multi-stage
- **Tipo:** infra · **Fase:** 6 · **Depende de:** SHP-02, SHP-21
- **Contexto:** a mesma imagem serve a API e o worker, mudando só o comando de inicialização. A imagem precisa de PHP-FPM ou servidor embutido, da extensão `rdkafka`, do `pcntl` (sinais) e das dependências de produção do Composer (sem dev). Otimize autoload e caches de configuração no build. Rode sem privilégios. O contexto de build é a pasta do serviço (o `Dockerfile` fica nela e nada é copiado de fora), para que `docker build` funcione de dentro dela.
- **Pronto quando:** a imagem sobe como API (`/health` 200) e, com outro comando, como worker (aparece no consumer group); a imagem final não tem ferramentas de dev; configuração só por env.
- **Referência:** guia 01, seções 7.5 e 12 (fase 6).
