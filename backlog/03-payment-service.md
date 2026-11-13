# PAY — payment-service (Java / Spring Boot)

Bounded context **Payments**. Consome `stock.reserved`, cobra num gateway fake (recusa acima de R$ 1000) e publica `payment.approved` ou `payment.rejected`. Fase 3, que fecha a saga completa.

Referências gerais: guia 01 (seções 3, 4, 6, 7.3, 8) e guia 02 (seção 3, Java + Spring Boot).

---

## PAY-E1 — Setup (Fase 3)

**Progresso:**
- [ ] PAY-01 Criar o projeto e a estrutura de pacotes
- [ ] PAY-02 Configuração e health

### PAY-01 — Criar o projeto e a estrutura de pacotes
- **Tipo:** setup · **Fase:** 3 · **Depende de:** INF-01
- **Contexto:** gere o projeto no start.spring.io (Java 21, Maven, dependências: Web, Validation, Data JPA, PostgreSQL, Spring for Apache Kafka, Actuator, Flyway, Testcontainers). Monte os pacotes da seção 7.3 (`domain`, `application`, `infra`, `presentation`) lembrando que pacote Java não aceita hífen. O serviço é autocontido: crie dentro da pasta dele um `.gitignore` e um `.dockerignore` próprios, um `.env.example` com todas as variáveis e um `README.md` curto (como rodar, testar e buildar), sem depender de arquivos da raiz.
- **Pronto quando:** `./mvnw spring-boot:run` sobe (mesmo sem banco, se necessário desligando a auto-configuração temporariamente) e `./mvnw test` roda; a estrutura de pastas bate com a seção 7.3. A pasta do serviço, copiada sozinha para outro lugar, roda, testa e builda sem arquivos de fora dela; existem `.gitignore`, `.dockerignore`, `.env.example` e `README.md` na pasta.
- **Referência:** guia 01, seção 7.3; guia 02, seção 3 (ferramentas e comandos).

### PAY-02 — Configuração e health
- **Tipo:** setup · **Fase:** 3 · **Depende de:** PAY-01
- **Contexto:** `application.yml` com tudo lido de variáveis de ambiente com valores padrão para dev (datasource, brokers, group id), `ddl-auto: validate`, virtual threads ligadas e Actuator expondo só `health`.
- **Pronto quando:** `GET /actuator/health` responde 200 na porta 8080; mudar o broker por variável de ambiente funciona.
- **Referência:** guia 01, seção 3.4; guia 02, seção 3 (arquivo principal).

---

## PAY-E2 — Domínio Payments (Fase 3)

**Progresso:**
- [ ] PAY-03 Value Objects `Money` e `PaymentStatus`
- [ ] PAY-04 Aggregate `Payment`

### PAY-03 — Value Objects `Money` e `PaymentStatus`
- **Tipo:** domain · **Fase:** 3 · **Depende de:** PAY-01
- **Contexto:** `Money` como `record` com `BigDecimal` criado a partir de string (nunca `double`), validação no compact constructor e operações que devolvem novo objeto. `PaymentStatus` com `APPROVED` e `REJECTED`. Lembre que comparação de `BigDecimal` é por `compareTo`.
- **Pronto quando:** testes (JUnit 5 + AssertJ): valor negativo lança exceção de domínio; `2.0` e `2.00` são tratados como iguais pela regra do VO; soma não perde precisão; status só aceita os dois valores.
- **Referência:** guia 01, seção 4; guia 02, seção 3 (tipos e records).

### PAY-04 — Aggregate `Payment`
- **Tipo:** domain · **Fase:** 3 · **Depende de:** PAY-03
- **Contexto:** `Payment` tem `orderId`, `amount` > 0 e status. Fábricas para aprovado e rejeitado (a partir do resultado do gateway), e uma fábrica de reconstituição para o repositório. Registra `PaymentApproved` ou `PaymentRejected` (com motivo). Base de aggregate com `pullEvents`.
- **Pronto quando:** testes: aprovado e rejeitado a partir do resultado do gateway; `amount` ≤ 0 rejeitado; evento correto registrado; `pullEvents` entrega e limpa; o domínio não importa Spring nem JPA.
- **Referência:** guia 01, seções 4 e 8; guia 02, seção 3 (classes e herança).

---

## PAY-E3 — Casos de uso (Fase 3)

**Progresso:**
- [ ] PAY-05 Ports e repositório em memória
- [ ] PAY-06 `FakePaymentGateway`
- [ ] PAY-07 `ProcessPaymentUseCase`
- [ ] PAY-08 `GetPaymentUseCase`
- [ ] PAY-09 `UseCaseConfig`

### PAY-05 — Ports e repositório em memória
- **Tipo:** use-case · **Fase:** 3 · **Depende de:** PAY-04
- **Contexto:** defina `PaymentRepository` (domínio), `PaymentGateway` e `EventPublisher` (application), o resultado do gateway como tipo selado (aprovado/rejeitado com motivo) e um `InMemoryPaymentRepository` usado pelos testes. Inclua um port de "eventos processados".
- **Pronto quando:** o repositório em memória salva e busca por `orderId`; o pacote `application` não importa nada do Spring.
- **Referência:** guia 01, seções 4 e 6; guia 02, seção 3 (interfaces, sealed e injeção).

### PAY-06 — `FakePaymentGateway`
- **Tipo:** use-case · **Fase:** 3 · **Depende de:** PAY-05
- **Contexto:** adapter determinístico: recusa quando `amount > 1000`, aprova caso contrário (exatamente 1000 aprova). Fica na infra, mas é testado como unidade.
- **Pronto quando:** testes: 100 aprova; 1000 aprova; 1000,01 recusa com motivo "limit exceeded" ou equivalente.
- **Referência:** guia 01, seções 4 e 8.

### PAY-07 — `ProcessPaymentUseCase`
- **Tipo:** use-case · **Fase:** 3 · **Depende de:** PAY-05, PAY-06
- **Contexto:** recebe `orderId` e `total` (vindos de `stock.reserved`), cobra no gateway, cria o `Payment`, salva e publica o evento. Um pagamento por `orderId`: repetir o pedido não cobra duas vezes e devolve o resultado já existente sem republicar.
- **Pronto quando:** testes (Mockito só para o publisher, repositório em memória): aprovado salva e publica `payment.approved`; recusado publica `payment.rejected` com motivo; segunda chamada com o mesmo `orderId` não cobra nem publica de novo; o gateway só é chamado uma vez.
- **Referência:** guia 01, seção 8; guia 02, seção 3 (testes).

### PAY-08 — `GetPaymentUseCase`
- **Tipo:** use-case · **Fase:** 3 · **Depende de:** PAY-05
- **Contexto:** busca o pagamento por `orderId` e devolve um DTO (`record`) com `id`, `orderId`, `amount` e `status`. Crie `PaymentNotFoundException`.
- **Pronto quando:** testes: devolve o pagamento existente; inexistente lança `PaymentNotFoundException`.
- **Referência:** guia 01, seção 3.4.

### PAY-09 — `UseCaseConfig`
- **Tipo:** use-case · **Fase:** 3 · **Depende de:** PAY-07, PAY-08
- **Contexto:** os casos de uso são Java puro, sem `@Service`. Registre-os como `@Bean` numa classe de configuração na infra, injetando os ports.
- **Pronto quando:** o contexto do Spring sobe resolvendo todos os beans (um teste de contexto simples, com adapters em memória, prova isso); nenhuma classe de `application` tem anotação do Spring.
- **Referência:** guia 01, seção 7.3 (nota); guia 02, seção 3 (injeção de dependência).

---

## PAY-E4 — API HTTP (Fase 3)

**Progresso:**
- [ ] PAY-10 Controller e tratamento de erros
- [ ] PAY-11 Teste da camada web

### PAY-10 — Controller e tratamento de erros
- **Tipo:** api · **Fase:** 3 · **Depende de:** PAY-08, PAY-09
- **Contexto:** `GET /payments?orderId=...` com `@RestController` e um `@RestControllerAdvice` que mapeia `PaymentNotFoundException` para 404 (`PAYMENT_NOT_FOUND`) e parâmetro ausente para 400, no formato `{ error, message }`.
- **Pronto quando:** com o app no ar e um pagamento no repositório em memória, a consulta devolve 200 com o DTO; inexistente devolve 404 no formato padrão.
- **Referência:** guia 01, seções 3.4 e 13; guia 02, seção 3 (annotations e erros).

### PAY-11 — Teste da camada web
- **Tipo:** test-e2e · **Fase:** 3 · **Depende de:** PAY-10
- **Contexto:** `@WebMvcTest(PaymentController.class)` com o caso de uso substituído (`@MockitoBean`).
- **Pronto quando:** testes: 200 com o corpo esperado; 404 para pedido sem pagamento; 400 sem `orderId`; o formato do erro segue a convenção.
- **Referência:** guia 01, seção 8; guia 02, seção 3 (testes).

---

## PAY-E5 — Persistência em Postgres (Fase 3)

**Progresso:**
- [ ] PAY-12 Migrations com Flyway
- [ ] PAY-13 Entidade JPA, mapper e repositórios
- [ ] PAY-14 Teste de integração com Testcontainers

### PAY-12 — Migrations com Flyway
- **Tipo:** persistence · **Fase:** 3 · **Depende de:** PAY-02, INF-02
- **Contexto:** versionar o schema (tabela de pagamentos com restrição única em `order_id` e tabela de eventos processados). Com `ddl-auto: validate`, o Hibernate só confere o schema.
- **Pronto quando:** subir o app contra a database `payments` cria as tabelas via Flyway; mudar uma coluna sem migration faz o `validate` falhar.
- **Referência:** guia 01, seção 7.3; guia 02, seção 3 (arquivo principal).

### PAY-13 — Entidade JPA, mapper e repositórios
- **Tipo:** persistence · **Fase:** 3 · **Depende de:** PAY-12, PAY-05
- **Contexto:** `PaymentJpaEntity` e o repositório Spring Data ficam na infra; `JpaPaymentRepository` implementa o port do domínio convertendo nos dois sentidos. A entidade de domínio não recebe nenhuma anotação JPA. Faça o mesmo para eventos processados.
- **Pronto quando:** o adapter compila contra o port; a restrição única em `order_id` protege contra pagamento duplicado mesmo em concorrência.
- **Referência:** guia 01, seção 7.3 (nota sobre JPA); guia 02, seção 3.

### PAY-14 — Teste de integração com Testcontainers
- **Tipo:** test-integration · **Fase:** 3 · **Depende de:** PAY-13
- **Contexto:** `@SpringBootTest` com `PostgreSQLContainer` e `@ServiceConnection`; nome da classe termina em `IT` para rodar no `verify`.
- **Pronto quando:** testes: salvar e recuperar por `orderId`; duplicidade de `orderId` é rejeitada pelo banco; evento processado repetido é detectado; mesma suíte de comportamento passa no repositório em memória.
- **Referência:** guia 01, seção 8; guia 02, seção 3 (testes).

---

## PAY-E6 — Mensageria Kafka e saga (Fase 3)

**Progresso:**
- [ ] PAY-15 `KafkaEventPublisher`
- [ ] PAY-16 Listener de `stock.reserved`
- [ ] PAY-17 Integração com Kafka
- [ ] PAY-18 Validar a saga completa

### PAY-15 — `KafkaEventPublisher`
- **Tipo:** messaging · **Fase:** 3 · **Depende de:** PAY-05, INF-06
- **Contexto:** `KafkaTemplate` enviando `payment.approved` e `payment.rejected` no envelope padrão, com chave `orderId`. A serialização do envelope é responsabilidade da infra.
- **Pronto quando:** publicar um evento deixa no tópico uma mensagem com envelope completo e a chave `orderId`.
- **Referência:** guia 01, seções 3.1 e 3.2; guia 02, seção 3 (Kafka).

### PAY-16 — Listener de `stock.reserved`
- **Tipo:** messaging · **Fase:** 3 · **Depende de:** PAY-07, PAY-13, PAY-15
- **Contexto:** `@KafkaListener` no group `payment-service` que desserializa o envelope, checa o `eventId`, chama o caso de uso e confirma o offset só depois do sucesso. Defina o comportamento para mensagem malformada (descartar com log) e para falha transitória (retry limitado).
- **Pronto quando:** publicar `stock.reserved` manualmente gera um `Payment` no banco e um `payment.approved` (ou `payment.rejected` se o total > 1000); repetir a mesma mensagem não cobra duas vezes.
- **Referência:** guia 01, seção 6 (fluxo de uma mensagem); guia 02, seção 3 (annotations).

### PAY-17 — Integração com Kafka
- **Tipo:** test-integration · **Fase:** 3 · **Depende de:** PAY-16
- **Contexto:** Testcontainers com Kafka e Postgres; produza `stock.reserved` e consuma o que sai.
- **Pronto quando:** testes: total 100 → `payment.approved`; total 2000 → `payment.rejected`; evento duplicado → uma única saída; mensagem malformada não trava o listener.
- **Referência:** guia 01, seção 8.

### PAY-18 — Validar a saga completa
- **Tipo:** exercise · **Fase:** 3 · **Depende de:** PAY-16, ORD-29, CAT-23
- **Contexto:** com order, catalog e payment no ar contra o mesmo Kafka, rode os dois caminhos da saga e acompanhe a sequência de eventos na Kafka UI.
- **Pronto quando:** **critério de pronto da fase 3:** pedido de R$ 100 termina `CONFIRMED`; pedido de R$ 2000 termina `CANCELLED` e o estoque volta; pedido com item sem estoque termina `CANCELLED` sem reserva para liberar.
- **Referência:** guia 01, seções 2.2 e 12 (fase 3).

---

## PAY-E7 — Container (Fase 6)

**Progresso:**
- [ ] PAY-19 Dockerfile multi-stage

### PAY-19 — Dockerfile multi-stage
- **Tipo:** infra · **Fase:** 6 · **Depende de:** PAY-02
- **Contexto:** estágio de build com JDK gerando o jar e estágio final só com o JRE. Ajuste memória da JVM para container, rode sem privilégios e use o Actuator como health check. Avalie `spring-boot:build-image` como comparação. O contexto de build é a pasta do serviço (o `Dockerfile` fica nela e nada é copiado de fora), para que `docker build` funcione de dentro dela.
- **Pronto quando:** a imagem sobe e `/actuator/health` responde 200; configuração só por env; a imagem final não tem o JDK nem o código-fonte.
- **Referência:** guia 01, seção 12 (fase 6); guia 02, seção 3 (ferramentas e comandos).
