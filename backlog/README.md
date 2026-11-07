# Backlog do ShopFlow — épicos e tasks

Este backlog transforma os guias [`01-arquitetura-e-roadmap.md`](../01-arquitetura-e-roadmap.md) e [`02-guia-linguagens.md`](../02-guia-linguagens.md) em trabalho executável. Cada **épico** agrupa um tema de um serviço; cada **task** é um passo de tamanho médio (algumas horas), com ponto de partida e critério de pronto. Nenhuma task traz código: o código é seu.

## Arquivos

| Arquivo | Prefixo | Conteúdo | Fases |
|---|---|---|---|
| [`00-infra-local.md`](./00-infra-local.md) | `INF` | Docker Compose: infra local e sistema completo | 0 e 6 |
| [`01-order-service.md`](./01-order-service.md) | `ORD` | NestJS / TypeScript, contexto Ordering | 1 a 4 e 6 |
| [`02-catalog-service.md`](./02-catalog-service.md) | `CAT` | FastAPI / Python, contexto Catalog & Inventory | 2, 3 e 6 |
| [`03-payment-service.md`](./03-payment-service.md) | `PAY` | Spring Boot / Java, contexto Payments | 3 e 6 |
| [`04-notification-service.md`](./04-notification-service.md) | `NOT` | Go, contexto Notification | 4 e 6 |
| [`05-shipping-service.md`](./05-shipping-service.md) | `SHP` | Laravel / PHP, contexto Shipping | 5 e 6 |
| [`06-kubernetes.md`](./06-kubernetes.md) | `K8S` | Plataforma, apps e exercícios no kind | 7 |
| [`07-terraform.md`](./07-terraform.md) | `TF` | Plataforma como código | 8 |

Os extras (fase 9 do roadmap) ficam fora: escolha um deles quando chegar lá e peça os épicos.

## Como ler uma task

- **Tipo:** `setup`, `domain`, `use-case`, `api`, `persistence`, `messaging`, `test-integration`, `test-e2e`, `infra` ou `exercise`.
- **Fase:** a fase do roadmap (seção 12 do guia 01) em que a task deve ser feita.
- **Depende de:** IDs que precisam estar prontos antes. Dependência entre serviços aparece explícita (por exemplo, `CAT-22` depende de `ORD-27`).
- **Contexto:** o que fazer e por quê, sem receita.
- **Pronto quando:** critérios que você consegue verificar (teste que existe e passa, comportamento observável).
- **Referência:** onde ler no guia.

IDs: `<PREFIXO>-E<n>` é épico e `<PREFIXO>-<nn>` é task. A numeração das tasks é contínua dentro do serviço e **segue a ordem sugerida de execução**.

## Regras do jogo

1. **TDD nas camadas de domínio e de casos de uso:** o teste unitário faz parte da mesma task da implementação. Escreva o teste, veja falhar pelo motivo certo, faça passar, refatore.
2. **Integração e e2e são tasks próprias** (`test-integration`, `test-e2e`), porque exigem containers e demoram mais.
3. **Uma fase só fecha** quando as tasks de validação dela (`exercise`) passam. Não avance antes.
4. **Marque** `[x]` nos checklists de progresso de cada épico conforme termina.
5. Commits seguem a convenção da seção 13 do guia 01 (`feat(order): ...`, `test(catalog): ...`).

## Ordem de execução por fase

| Fase | Foco | Tasks |
|---|---|---|
| 0 | Fundamentos e infra | `INF-01` a `INF-08` |
| 1 | order-service sozinho | `ORD-01` a `ORD-11`, `ORD-15` a `ORD-23` |
| 2 | catalog + primeiro Kafka | `CAT-01` a `CAT-10`, `CAT-12` a `CAT-22`, `CAT-25`, `ORD-25` a `ORD-27` |
| 3 | payment + saga completa | `PAY-01` a `PAY-18`, `ORD-12` a `ORD-14`, `ORD-24`, `ORD-28` a `ORD-30`, `CAT-11`, `CAT-23`, `CAT-24` |
| 4 | notification + RabbitMQ | `NOT-01` a `NOT-13`, `ORD-31` a `ORD-33` |
| 5 | shipping (Laravel) | `SHP-01` a `SHP-25` |
| 6 | containers | `ORD-34`, `CAT-26`, `PAY-19`, `NOT-14`, `SHP-26`, `INF-09` a `INF-12` |
| 7 | Kubernetes | `K8S-01` a `K8S-18` |
| 8 | Terraform | `TF-01` a `TF-13` |

Dentro de uma fase, alterne entre serviços só quando uma dependência pedir. O caminho crítico das primeiras fases é: `ORD-23` (order persistindo) → `ORD-27` (order publicando no Kafka) → `CAT-22` (catalog reservando) → `PAY-16` (payment cobrando) → `ORD-29` (order confirmando).

## Dependências entre serviços (visão rápida)

| Quem espera | Espera por | Motivo |
|---|---|---|
| `CAT-21`, `CAT-22` | `ORD-25`, `ORD-27` | O catalog consome `order.created` publicado pelo order |
| `PAY-16` | `CAT-21` | O payment consome `stock.reserved` publicado pelo catalog |
| `ORD-28`, `ORD-29` | `CAT-21`, `PAY-15` | O order consome eventos de estoque e pagamento |
| `CAT-23` | `ORD-27` | O catalog consome `order.cancelled` |
| `SHP-21` | `ORD-12` | O shipping precisa do `customerEmail` em `order.confirmed` |
| `NOT-13`, `SHP-25` | `ORD-33`, `NOT-04` | Comandos de e-mail precisam chegar e o template `order-shipped` precisa existir |
| `K8S-07` a `K8S-11` | `*-34`/`*-26`/`*-19`/`*-14` | Cada app precisa da imagem Docker |
| `TF-01` | `K8S-14` | Faça na mão antes de virar código |

## Publicar como GitHub Issues

Os arquivos seguem um formato fixo (títulos `##` para épicos e `###` para tasks, campos em negrito) para um script poder converter cada seção em issue, com labels por serviço e tipo e uma milestone por fase.
