# CAT — catalog-service (FastAPI / Python)

Bounded context **Catalog & Inventory**. Cadastra produtos, reserva estoque quando um pedido é criado e libera quando é cancelado (compensação da saga). Fase principal: 2, com a compensação na 3.

Referências gerais: guia 01 (seções 3, 4, 6, 7.2, 8) e guia 02 (seção 2, Python + FastAPI).

---

## CAT-E1 — Setup (Fase 2)

**Progresso:**
- [ ] CAT-01 Criar o projeto e a estrutura de pastas
- [ ] CAT-02 Configuração por env e `/health`

### CAT-01 — Criar o projeto e a estrutura de pastas
- **Tipo:** setup · **Fase:** 2 · **Depende de:** INF-01
- **Contexto:** inicialize o projeto com `uv`, adicione FastAPI e as dependências de dev (pytest, pytest-asyncio, httpx, testcontainers, mypy, ruff). Monte a árvore da seção 7.2 com os `__init__.py` e configure `asyncio_mode` do pytest, ruff e mypy no `pyproject.toml`.
- **Pronto quando:** `uv run fastapi dev app/main.py` sobe, `uv run pytest` roda, `uv run ruff check .` e `uv run mypy app` passam no projeto vazio.
- **Referência:** guia 01, seção 7.2; guia 02, seção 2 (ferramentas e comandos).

### CAT-02 — Configuração por env e `/health`
- **Tipo:** setup · **Fase:** 2 · **Depende de:** CAT-01
- **Contexto:** crie `config.py` com `pydantic-settings` (URL do banco, brokers do Kafka, group id, nomes de tópicos). Adicione `GET /health` direto no `main.py`, com o app já usando `lifespan` (vazio por enquanto).
- **Pronto quando:** `/health` retorna 200 `{status: ok}`; trocar uma variável de ambiente muda a configuração lida; existe um teste do `/health` com `TestClient`.
- **Referência:** guia 01, seção 3.4; guia 02, seção 2 (arquivo principal).

---

## CAT-E2 — Domínio Catalog & Inventory (Fase 2)

**Progresso:**
- [ ] CAT-03 Base compartilhada e Value Objects `Money` e `Quantity`
- [ ] CAT-04 Aggregate `Product`
- [ ] CAT-05 Aggregate `Reservation`
- [ ] CAT-06 Domain service `StockReservationService`

### CAT-03 — Base compartilhada e Value Objects `Money` e `Quantity`
- **Tipo:** domain · **Fase:** 2 · **Depende de:** CAT-01
- **Contexto:** em `shared/domain`, a base de aggregate (acumula e entrega eventos), o evento base e o erro base. Depois dois VOs imutáveis (dataclass frozen): `Money` (Decimal criado a partir de string, não negativo) e `Quantity` (inteiro positivo).
- **Pronto quando:** testes: valores válidos; negativo/zero rejeitado conforme a regra de cada VO; igualdade por valor; imutabilidade (tentar alterar falha); `float` não é aceito para dinheiro.
- **Referência:** guia 01, seções 4 e 8; guia 02, seção 2 (tipos e dataclass).

### CAT-04 — Aggregate `Product`
- **Tipo:** domain · **Fase:** 2 · **Depende de:** CAT-03
- **Contexto:** `Product` tem nome não vazio, preço > 0 e estoque ≥ 0. `reserve(qty)` diminui o estoque e falha com `InsufficientStock` se `qty > stock`; `release(qty)` devolve. Identidade por id.
- **Pronto quando:** testes: rejeita preço ≤ 0 e estoque negativo (parametrizado); `reserve` diminui; `reserve` acima do estoque lança `InsufficientStock` sem alterar o estoque; `release` devolve; ajuste de estoque (reposição) respeita a regra de não ficar negativo.
- **Referência:** guia 01, seções 4 e 8; guia 02, seção 2 (classes, erros e pytest.parametrize).

### CAT-05 — Aggregate `Reservation`
- **Tipo:** domain · **Fase:** 2 · **Depende de:** CAT-03
- **Contexto:** uma reserva por `orderId`, guardando os itens reservados (para saber o que devolver) e o status `ACTIVE` ou `RELEASED`. Liberar uma reserva já liberada é um no-op ou erro (decida e documente), mas nunca devolve estoque duas vezes.
- **Pronto quando:** testes: nasce `ACTIVE` com os itens; `release()` marca `RELEASED`; liberar duas vezes não duplica efeito.
- **Referência:** guia 01, seção 4.

### CAT-06 — Domain service `StockReservationService`
- **Tipo:** domain · **Fase:** 2 · **Depende de:** CAT-04, CAT-05
- **Contexto:** regra que envolve vários produtos: reserva **todos os itens ou nenhum**. Se um item falhar, nada do que já foi reservado no mesmo pedido permanece. O serviço não persiste nem publica.
- **Pronto quando:** testes: todos os itens com estoque → todos reservados e uma `Reservation` ativa; um item sem estoque → estoque de todos intacto e erro indicando quais produtos falharam; liberar uma reserva devolve cada item.
- **Referência:** guia 01, seções 4 e 8.

---

## CAT-E3 — Casos de uso (Fase 2 e 3)

**Progresso:**
- [ ] CAT-07 Ports, repositórios em memória e eventos
- [ ] CAT-08 Casos de uso de leitura e cadastro de produtos
- [ ] CAT-09 `AdjustStock`
- [ ] CAT-10 `ReserveStock`
- [ ] CAT-11 `ReleaseStock`
- [ ] CAT-12 Idempotência por `eventId`

### CAT-07 — Ports, repositórios em memória e eventos
- **Tipo:** use-case · **Fase:** 2 · **Depende de:** CAT-05
- **Contexto:** crie os ABCs `ProductRepository` e `ReservationRepository`, o port `EventPublisher` (Protocol ou ABC), os eventos `StockReserved` e `StockFailed`, as versões em memória dos repositórios e um publisher falso que guarda o que recebeu.
- **Pronto quando:** repositórios em memória salvam e recuperam; busca de reserva por `orderId` funciona; nada em `application/` importa FastAPI ou SQLAlchemy.
- **Referência:** guia 01, seções 4, 5 e 6; guia 02, seção 2 (interfaces e herança).

### CAT-08 — Casos de uso de leitura e cadastro de produtos
- **Tipo:** use-case · **Fase:** 2 · **Depende de:** CAT-07
- **Contexto:** `CreateProduct`, `GetProduct` (com `ProductNotFound`) e `ListProducts`, com DTOs de saída. Use cases são classes Python puras que recebem as dependências pelo `__init__`.
- **Pronto quando:** testes: criar devolve id e dados; buscar existente devolve; inexistente lança `ProductNotFound`; listar devolve todos.
- **Referência:** guia 01, seção 3.4; guia 02, seção 2 (injeção de dependência).

### CAT-09 — `AdjustStock`
- **Tipo:** use-case · **Fase:** 2 · **Depende de:** CAT-07
- **Contexto:** reposição de estoque (`PATCH /products/{id}/stock`): aplica o ajuste no produto e salva. Defina se o corpo `{quantity}` é delta ou valor absoluto, e documente no contexto do endpoint.
- **Pronto quando:** testes: ajuste atualiza o estoque; produto inexistente lança `ProductNotFound`; ajuste que deixaria o estoque negativo é rejeitado.
- **Referência:** guia 01, seção 3.4.

### CAT-10 — `ReserveStock`
- **Tipo:** use-case · **Fase:** 2 · **Depende de:** CAT-06, CAT-07
- **Contexto:** entrada vem do evento `order.created` (itens e total). Usa o domain service; em sucesso salva produtos e reserva e publica `stock.reserved` com `orderId` e `total`; em falha **não altera estoque** e publica `stock.failed` com motivo e ids dos produtos. Para o mesmo `orderId` duas vezes, não reserva de novo.
- **Pronto quando:** testes: sucesso publica `stock.reserved` e reduz o estoque; falta em qualquer item publica `stock.failed` e o estoque permanece; segunda chamada para o mesmo `orderId` não reserva nem republica.
- **Referência:** guia 01, seções 2.2, 3.2 e 8.

### CAT-11 — `ReleaseStock`
- **Tipo:** use-case · **Fase:** 3 · **Depende de:** CAT-06, CAT-07
- **Contexto:** compensação disparada por `order.cancelled`. Se houver reserva `ACTIVE` para o `orderId`, devolve o estoque de cada item e marca `RELEASED`. Sem reserva (por exemplo, o pedido foi cancelado por falta de estoque) não faz nada.
- **Pronto quando:** testes: devolve o estoque e marca `RELEASED`; sem reserva não faz nada e não falha; chamar duas vezes não devolve em dobro.
- **Referência:** guia 01, seções 2.2 e 8.

### CAT-12 — Idempotência por `eventId`
- **Tipo:** use-case · **Fase:** 2 · **Depende de:** CAT-10
- **Contexto:** port de eventos processados (com versão em memória) usado pelos casos de uso que reagem a eventos, de modo que reprocessar a mesma mensagem não gera efeito. É uma defesa adicional à regra por `orderId`.
- **Pronto quando:** testes: o mesmo `eventId` em `ReserveStock` e em `ReleaseStock` só tem efeito uma vez; `eventId`s diferentes seguem normalmente.
- **Referência:** guia 01, seções 3.1 e 12 (fase 2).

---

## CAT-E4 — API HTTP (Fase 2)

**Progresso:**
- [ ] CAT-13 Router de produtos (criar e consultar)
- [ ] CAT-14 `PATCH /products/{id}/stock`
- [ ] CAT-15 Handlers de erro de domínio
- [ ] CAT-16 Testes e2e da API de produtos

### CAT-13 — Router de produtos (criar e consultar)
- **Tipo:** api · **Fase:** 2 · **Depende de:** CAT-08, CAT-02
- **Contexto:** `APIRouter` com `POST /products` (201), `GET /products` e `GET /products/{id}`. Schemas Pydantic para request e response, com restrições (`Field`) para nome, preço e estoque. `dependencies.py` monta os use cases com os repositórios em memória por enquanto.
- **Pronto quando:** as três rotas funcionam manualmente; payload inválido devolve erro de validação do FastAPI; o OpenAPI em `/docs` lista as rotas.
- **Referência:** guia 01, seção 3.4; guia 02, seção 2 (decorators e injeção de dependência).

### CAT-14 — `PATCH /products/{id}/stock`
- **Tipo:** api · **Fase:** 2 · **Depende de:** CAT-09, CAT-13
- **Contexto:** rota de reposição de estoque ligada ao `AdjustStock`, devolvendo o produto atualizado.
- **Pronto quando:** reposição funciona manualmente; produto inexistente devolve 404 após a CAT-15.
- **Referência:** guia 01, seção 3.4.

### CAT-15 — Handlers de erro de domínio
- **Tipo:** api · **Fase:** 2 · **Depende de:** CAT-13
- **Contexto:** `exception_handler` para `ProductNotFound` (404) e erros de regra (422/409), com corpo `{ error, message }`. Centralize o mapeamento.
- **Pronto quando:** o corpo de erro segue a convenção da seção 13; `GET /products/{id}` inexistente devolve 404 com `PRODUCT_NOT_FOUND`.
- **Referência:** guia 01, seção 13; guia 02, seção 2 (erros).

### CAT-16 — Testes e2e da API de produtos
- **Tipo:** test-e2e · **Fase:** 2 · **Depende de:** CAT-14, CAT-15
- **Contexto:** `TestClient` com `dependency_overrides` apontando para repositórios em memória.
- **Pronto quando:** testes: `POST /products` → 201; corpo inválido (preço ≤ 0, estoque negativo, nome vazio) → 422; `GET /products/{id}` existente → 200 e inexistente → 404; listagem traz o que foi criado; `PATCH` ajusta o estoque e 404 para id inexistente.
- **Referência:** guia 01, seção 8; guia 02, seção 2 (testes).

---

## CAT-E5 — Persistência em Postgres (Fase 2)

**Progresso:**
- [ ] CAT-17 SQLAlchemy e migrations
- [ ] CAT-18 Repositórios SQLAlchemy
- [ ] CAT-19 Testes de integração dos repositórios
- [ ] CAT-20 Ligar o Postgres e rodar o e2e

### CAT-17 — SQLAlchemy e migrations
- **Tipo:** persistence · **Fase:** 2 · **Depende de:** CAT-01, INF-02
- **Contexto:** modelos SQLAlchemy 2 async (produtos, reservas, itens da reserva, eventos processados), sessão assíncrona (`session.py`) e Alembic. Dinheiro como `Numeric`. Os modelos ficam na infra e não se confundem com as entidades.
- **Pronto quando:** `alembic upgrade head` cria as tabelas na database `catalog`; `alembic downgrade base` as remove.
- **Referência:** guia 01, seção 7.2; guia 02, seção 2 (ferramentas, migrations).

### CAT-18 — Repositórios SQLAlchemy
- **Tipo:** persistence · **Fase:** 2 · **Depende de:** CAT-17, CAT-07, CAT-12
- **Contexto:** implemente `ProductRepository`, `ReservationRepository` e o repositório de eventos processados, com o mapeamento modelo ↔ entidade. Reservar estoque e gravar a reserva precisam acontecer na **mesma transação**. Pense em concorrência: duas reservas ao mesmo tempo para o mesmo produto não podem estourar o estoque (bloqueio ou checagem na atualização).
- **Pronto quando:** os repositórios compilam contra os ABCs; uma sessão por request com commit/rollback centralizados em `dependencies.py`.
- **Referência:** guia 01, seção 6; guia 02, seção 2 (injeção com `yield`).

### CAT-19 — Testes de integração dos repositórios
- **Tipo:** test-integration · **Fase:** 2 · **Depende de:** CAT-18
- **Contexto:** Testcontainers com Postgres; rode a mesma suíte de comportamento nos repositórios em memória e nos SQLAlchemy.
- **Pronto quando:** testes: salvar e recuperar produto e reserva; buscar reserva por `orderId`; evento processado duplicado é detectado; duas reservas concorrentes do mesmo produto não deixam o estoque negativo; rollback desfaz tudo quando uma etapa falha.
- **Referência:** guia 01, seção 8.

### CAT-20 — Ligar o Postgres e rodar o e2e
- **Tipo:** persistence · **Fase:** 2 · **Depende de:** CAT-18, CAT-16
- **Contexto:** troque as dependências do FastAPI para os repositórios reais. Rode o e2e contra um Postgres real.
- **Pronto quando:** os e2e da CAT-16 passam contra banco real; os dados sobrevivem a um restart do app.
- **Referência:** guia 01, seção 12 (fase 2).

---

## CAT-E6 — Mensageria Kafka (Fase 2 e 3)

**Progresso:**
- [ ] CAT-21 `KafkaEventPublisher`
- [ ] CAT-22 Consumer de `order.created`
- [ ] CAT-23 Consumer de `order.cancelled`
- [ ] CAT-24 Testes de integração com Kafka
- [ ] CAT-25 Validar a fase 2

### CAT-21 — `KafkaEventPublisher`
- **Tipo:** messaging · **Fase:** 2 · **Depende de:** CAT-07, INF-06
- **Contexto:** publisher com `aiokafka` seguindo o envelope padrão e a chave `orderId`, para `stock.reserved` e `stock.failed`. Produtor iniciado e encerrado no `lifespan`.
- **Pronto quando:** publicar `StockReserved` coloca no tópico `stock.reserved` uma mensagem com envelope completo e chave `orderId` (conferir na Kafka UI).
- **Referência:** guia 01, seção 3.1 e 3.2; guia 02, seção 2 (Kafka).

### CAT-22 — Consumer de `order.created`
- **Tipo:** messaging · **Fase:** 2 · **Depende de:** CAT-10, CAT-12, CAT-21, CAT-20, ORD-27
- **Contexto:** `OrderEventsConsumer` com `AIOKafkaConsumer` (group `catalog-service`, auto-commit desligado) iniciado no `lifespan`. Para cada mensagem: desserializa, converte o evento externo no input do use case (camada anticorrupção), executa e **só então** faz commit. Mensagem malformada não pode travar a partição.
- **Pronto quando:** criar um pedido no order faz o catalog reservar o estoque e publicar `stock.reserved`; pedido com item sem estoque publica `stock.failed`; reiniciar o catalog não reprocessa mensagens já confirmadas.
- **Referência:** guia 01, seções 6 e 12 (fase 2); guia 02, seção 2 (Kafka e async).

### CAT-23 — Consumer de `order.cancelled`
- **Tipo:** messaging · **Fase:** 3 · **Depende de:** CAT-11, CAT-22, ORD-27
- **Contexto:** o mesmo consumer passa a assinar também `order.cancelled` e despachar para `ReleaseStock`. Roteie por tópico.
- **Pronto quando:** cancelar um pedido (por exemplo, publicando `payment.rejected` manualmente) devolve o estoque reservado; cancelamento de pedido sem reserva não gera erro.
- **Referência:** guia 01, seção 2.2.

### CAT-24 — Testes de integração com Kafka
- **Tipo:** test-integration · **Fase:** 3 · **Depende de:** CAT-22, CAT-23
- **Contexto:** Testcontainers com Kafka e Postgres: publique `order.created` e `order.cancelled` de verdade e verifique estoque e eventos de saída.
- **Pronto quando:** testes: evento de criação reserva e publica `stock.reserved`; falta de estoque publica `stock.failed` sem mexer no estoque; cancelamento devolve o estoque; mensagem duplicada não tem efeito; mensagem malformada não derruba o consumer.
- **Referência:** guia 01, seção 8.

### CAT-25 — Validar a fase 2
- **Tipo:** exercise · **Fase:** 2 · **Depende de:** CAT-22, ORD-27
- **Contexto:** com order, catalog, Kafka e Postgres no ar, cadastre produtos, crie pedidos, e acompanhe tudo na Kafka UI. Depois suba duas instâncias do catalog e observe como as partições se dividem no consumer group.
- **Pronto quando:** **critério de pronto da fase 2:** criar um pedido baixa o estoque no catalog e você vê `order.created` e `stock.reserved` (ou `stock.failed`) no Kafka UI; você explica o que acontece com a terceira instância quando há só 3 partições e 4 consumidores.
- **Referência:** guia 01, seções 10.3 e 12 (fase 2).

---

## CAT-E7 — Container (Fase 6)

**Progresso:**
- [ ] CAT-26 Dockerfile multi-stage

### CAT-26 — Dockerfile multi-stage
- **Tipo:** infra · **Fase:** 6 · **Depende de:** CAT-02
- **Contexto:** estágio de build instala dependências com `uv` e estágio final copia só o ambiente e o código. Roda sem privilégios e inicia com `fastapi run`. Migrations rodam em passo separado (job ou entrypoint explícito), decida e documente.
- **Pronto quando:** a imagem sobe e responde `/health`; é bem menor que a de build; configuração só por env.
- **Referência:** guia 01, seção 12 (fase 6); guia 02, seção 2 (ferramentas).
