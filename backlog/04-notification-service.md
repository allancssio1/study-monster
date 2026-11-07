# NOT — notification-service (Go)

Bounded context **Notification**. Worker sem banco e sem API de negócio: consome comandos de e-mail do RabbitMQ, "envia" (log) e trata falhas com retry e DLQ. Só expõe `/health`. Fase 4.

Referências gerais: guia 01 (seções 3.3, 4, 7.4, 8) e guia 02 (seção 4, Go).

---

## NOT-E1 — Setup (Fase 4)

**Progresso:**
- [ ] NOT-01 Criar o módulo e a estrutura de pastas
- [ ] NOT-02 Configuração e `/health`

### NOT-01 — Criar o módulo e a estrutura de pastas
- **Tipo:** setup · **Fase:** 4 · **Depende de:** INF-01
- **Contexto:** `go mod init` com o caminho do módulo, árvore da seção 7.4 (`cmd/notification`, `internal/config`, `internal/notification/{domain,application,infra,presentation}`) e um `main.go` mínimo. Em Go os testes ficam ao lado do código.
- **Pronto quando:** `go run ./cmd/notification` compila e termina sem erro; `go vet ./...` e `go test ./...` passam; `gofmt` não aponta diferenças.
- **Referência:** guia 01, seção 7.4; guia 02, seção 4 (arquivo principal e ferramentas).

### NOT-02 — Configuração e `/health`
- **Tipo:** setup · **Fase:** 4 · **Depende de:** NOT-01
- **Contexto:** `config.Load()` lendo variáveis de ambiente (URL do RabbitMQ, porta HTTP, nível de log) com padrões para dev. Logger estruturado com `log/slog`. Servidor HTTP na porta 8081 com `GET /health` usando o mux da stdlib, iniciado numa goroutine.
- **Pronto quando:** `GET /health` responde 200 `{"status":"ok"}`; há um teste do handler com `httptest`; trocar a porta por env funciona.
- **Referência:** guia 01, seção 3.4; guia 02, seção 4 (decorators/annotations e testes).

---

## NOT-E2 — Domínio e caso de uso (Fase 4)

**Progresso:**
- [ ] NOT-03 `Template`
- [ ] NOT-04 `Notification`
- [ ] NOT-05 Port `EmailSender` e caso de uso `SendNotification`
- [ ] NOT-06 `LogSender`

### NOT-03 — `Template`
- **Tipo:** domain · **Fase:** 4 · **Depende de:** NOT-01
- **Contexto:** tipo nomeado sobre string com os três templates conhecidos (`order-confirmed`, `order-cancelled`, `order-shipped`) e o método `IsValid`. Testes em tabela, o padrão de Go.
- **Pronto quando:** teste em tabela cobre os três válidos e pelo menos dois inválidos (vazio e desconhecido).
- **Referência:** guia 01, seções 3.3 e 4; guia 02, seção 4 (testes em tabela).

### NOT-04 — `Notification`
- **Tipo:** domain · **Fase:** 4 · **Depende de:** NOT-03
- **Contexto:** struct com campos privados e `NewNotification(to, template, data)` devolvendo `(*Notification, error)`. Valida o e-mail e o template, com erros sentinela (`ErrInvalidEmail`, `ErrUnknownTemplate`) comparáveis com `errors.Is`.
- **Pronto quando:** testes: e-mail inválido → `ErrInvalidEmail`; template desconhecido → `ErrUnknownTemplate`; válido devolve a notificação com os getters corretos; `order-shipped` é aceito.
- **Referência:** guia 01, seções 4 e 8; guia 02, seção 4 (structs e erros).

### NOT-05 — Port `EmailSender` e caso de uso `SendNotification`
- **Tipo:** use-case · **Fase:** 4 · **Depende de:** NOT-04
- **Contexto:** a interface `EmailSender` é definida em `application/ports.go`, por quem a usa e pequena. O caso de uso constrói a `Notification` a partir do input e chama o sender com `context`. Teste com um fake escrito à mão (sem framework de mock).
- **Pronto quando:** testes: chama o sender com os dados certos; propaga o erro do sender; entrada inválida retorna erro do domínio e **não** chama o sender.
- **Referência:** guia 01, seção 8; guia 02, seção 4 (interfaces).

### NOT-06 — `LogSender`
- **Tipo:** use-case · **Fase:** 4 · **Depende de:** NOT-05
- **Contexto:** adapter que escreve o e-mail no log estruturado e **falha** quando o destinatário termina em `@fail.com` (para provocar o fluxo de retry e DLQ). Garanta em tempo de compilação que satisfaz a interface.
- **Pronto quando:** testes: envia normalmente e loga os campos; `@fail.com` devolve erro; o log não vaza o conteúdo inteiro de `data` se você decidir mascarar.
- **Referência:** guia 01, seção 3.3; guia 02, seção 4 (interfaces).

---

## NOT-E3 — Consumo RabbitMQ (Fase 4)

**Progresso:**
- [ ] NOT-07 Topologia
- [ ] NOT-08 Decisão por mensagem (ack, retry ou DLQ)
- [ ] NOT-09 Consumer com ack manual
- [ ] NOT-10 Retry e DLQ
- [ ] NOT-11 Teste de integração
- [ ] NOT-12 Graceful shutdown
- [ ] NOT-13 Validar a fase 4

### NOT-07 — Topologia
- **Tipo:** messaging · **Fase:** 4 · **Depende de:** NOT-02, INF-05
- **Contexto:** na inicialização, declare de forma idempotente as exchanges `notifications` e `notifications.dlx`, a fila `notifications.email` (com dead-letter para a DLX) e a fila `notifications.email.dlq`, com os bindings e a routing key `email.send`. Declare parâmetros iguais aos que o painel mostrou no exercício da fase 0, ou o broker rejeita.
- **Pronto quando:** subir o app duas vezes seguidas não gera erro; o painel mostra a topologia completa, com a fila principal indicando a DLX.
- **Referência:** guia 01, seção 3.3; guia 02, seção 4 (RabbitMQ).

### NOT-08 — Decisão por mensagem (ack, retry ou DLQ)
- **Tipo:** use-case · **Fase:** 4 · **Depende de:** NOT-05
- **Contexto:** isole a lógica de decisão numa função testável **sem broker**: dado o corpo da mensagem, o contador `x-retry-count` e o resultado do caso de uso, decide entre ack, republicar com contador + 1 ou enviar para a DLQ. JSON inválido e erros de validação são permanentes (DLQ direto); erro do sender é transitório (retry até 3).
- **Pronto quando:** testes em tabela cobrem: sucesso → ack; falha com retry < 3 → republicar com +1; falha com retry = 3 → DLQ; JSON inválido → DLQ sem retry; template desconhecido → DLQ sem retry.
- **Referência:** guia 01, seções 3.3 e 8.

### NOT-09 — Consumer com ack manual
- **Tipo:** messaging · **Fase:** 4 · **Depende de:** NOT-07, NOT-08
- **Contexto:** `RabbitMQConsumer` com `amqp091-go`: `Qos` com prefetch 10, `Consume` com auto-ack desligado e laço com `select` entre `ctx.Done()` e o canal de entregas. A mensagem só é confirmada depois de processada. Trate o fechamento do canal.
- **Pronto quando:** publicar uma mensagem válida no painel produz o log do e-mail e a mensagem sai da fila (ack); a conexão perdida resulta em erro retornado, não em loop silencioso.
- **Referência:** guia 01, seção 3.3; guia 02, seção 4 (concorrência e RabbitMQ).

### NOT-10 — Retry e DLQ
- **Tipo:** messaging · **Fase:** 4 · **Depende de:** NOT-09
- **Contexto:** aplique a decisão da NOT-08 ao consumer: falha transitória republica na exchange com `x-retry-count + 1` e confirma a original; no limite, `nack` sem requeue (a DLX leva para a DLQ); mensagem inválida vai direto para a DLQ.
- **Pronto quando:** uma mensagem para `x@fail.com` passa por 3 retentativas e termina em `notifications.email.dlq`, visível no painel com o contador no header; JSON inválido aparece na DLQ na primeira tentativa.
- **Referência:** guia 01, seção 3.3; guia 02, seção 4.

### NOT-11 — Teste de integração
- **Tipo:** test-integration · **Fase:** 4 · **Depende de:** NOT-10, NOT-06
- **Contexto:** `testcontainers-go` com RabbitMQ, protegido por `testing.Short()` ou build tag para não rodar sempre.
- **Pronto quando:** testes: sucesso → ack e fila vazia; falha com retry < 3 → republica com contador + 1; falha com retry = 3 → DLQ; JSON inválido → DLQ direto; mensagem persistente sobrevive a um restart do consumer.
- **Referência:** guia 01, seção 8; guia 02, seção 4 (testes).

### NOT-12 — Graceful shutdown
- **Tipo:** messaging · **Fase:** 4 · **Depende de:** NOT-09
- **Contexto:** no `main`, contexto cancelado por `SIGINT`/`SIGTERM` (o Kubernetes manda `SIGTERM`). Ao cancelar: para de aceitar novas entregas, termina a mensagem em andamento, fecha canal e conexão e encerra o servidor HTTP.
- **Pronto quando:** `Ctrl+C` com uma mensagem em processamento termina a mensagem, confirma e só então sai com código 0; nenhuma mensagem fica "unacked" presa depois da saída; teste com `-race` passa.
- **Referência:** guia 01, seção 12 (fase 4); guia 02, seção 4 (arquivo principal).

### NOT-13 — Validar a fase 4
- **Tipo:** exercise · **Fase:** 4 · **Depende de:** NOT-11, ORD-33
- **Contexto:** com order e notification no ar, confirme e cancele pedidos e teste falhas. Derrube o notification, gere pedidos, suba de novo e veja as mensagens esperando na fila.
- **Pronto quando:** **critério de pronto da fase 4:** pedido confirmado gera o log de e-mail; `to` com `@fail.com` termina na DLQ após 3 tentativas; mensagens geradas com o notification parado são consumidas quando ele volta.
- **Referência:** guia 01, seções 10.3 e 12 (fase 4).

---

## NOT-E4 — Container (Fase 6)

**Progresso:**
- [ ] NOT-14 Dockerfile multi-stage

### NOT-14 — Dockerfile multi-stage
- **Tipo:** infra · **Fase:** 6 · **Depende de:** NOT-02
- **Contexto:** estágio de build compilando um binário estático e estágio final em `distroless` (ou `scratch`) rodando sem privilégios. Como a imagem não tem shell, o health check é feito pelo orquestrador via HTTP.
- **Pronto quando:** a imagem sobe, responde `/health` e tem poucas dezenas de MB; recebe `SIGTERM` e encerra com elegância (consumer parando) em vez de ser morta.
- **Referência:** guia 01, seção 12 (fase 6); guia 02, seção 4 (ferramentas).
