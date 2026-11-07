# K8S — Kubernetes (kind)

Levar tudo para um cluster local: primeiro a **plataforma** (cluster, ingress, Kafka, RabbitMQ, Postgres), depois os **apps** e por fim os exercícios de operação. Faça tudo **na mão** (YAML e `kubectl`); a fase 8 reescreve a plataforma como código. Fase 7.

Referências gerais: guia 01, seção 10 (Kubernetes) e seção 12 (fase 7).

Pré-requisito geral: as imagens Docker de todos os serviços (fase 6) e o compose validado (`INF-12`).

---

## K8S-E1 — Plataforma (Fase 7)

Cluster e dependências de infraestrutura rodando no namespace do projeto.

**Progresso:**
- [ ] K8S-01 Cluster kind
- [ ] K8S-02 ingress-nginx
- [ ] K8S-03 Namespace
- [ ] K8S-04 Kafka com Strimzi
- [ ] K8S-05 RabbitMQ com operator
- [ ] K8S-06 Postgres

### K8S-01 — Cluster kind
- **Tipo:** infra · **Fase:** 7 · **Depende de:** INF-12
- **Contexto:** crie o cluster com um arquivo de configuração que mapeia as portas 80 e 443 do host para o nó de ingress (label `ingress-ready`). Guarde o arquivo em `k8s/`.
- **Pronto quando:** `kubectl get nodes` mostra o nó `Ready`; o contexto do `kubectl` aponta para o cluster; `kind delete cluster` e recriar leva um comando.
- **Referência:** guia 01, seção 10.2 (passo 1).

### K8S-02 — ingress-nginx
- **Tipo:** infra · **Fase:** 7 · **Depende de:** K8S-01
- **Contexto:** instale o manifest do ingress-nginx específico para kind (veja a documentação do kind) e espere o controller ficar pronto.
- **Pronto quando:** o pod do controller está `Running`; `curl localhost` devolve a página 404 do nginx (ainda sem rotas).
- **Referência:** guia 01, seção 10.2 (passo 2).

### K8S-03 — Namespace
- **Tipo:** infra · **Fase:** 7 · **Depende de:** K8S-01
- **Contexto:** crie `k8s/namespace.yaml` com o namespace `shopflow` e use-o em todos os recursos seguintes.
- **Pronto quando:** `kubectl get ns shopflow` existe; todos os próximos manifests declaram o namespace.
- **Referência:** guia 01, seção 10.1.

### K8S-04 — Kafka com Strimzi
- **Tipo:** infra · **Fase:** 7 · **Depende de:** K8S-03
- **Contexto:** instale o operator Strimzi, crie o recurso `Kafka` em modo KRaft (com o node pool necessário na versão que usar) e um `KafkaTopic` para cada um dos 8 tópicos, com 3 partições e replication factor 1. Guarde em `k8s/kafka/`.
- **Pronto quando:** o pod do broker está `Ready`; `kubectl get kafkatopic -n shopflow` lista os 8 tópicos; você produz e consome uma mensagem de teste de dentro do cluster. Anote o endereço de bootstrap interno que os serviços usarão.
- **Referência:** guia 01, seção 10.2 (passo 3); referência Strimzi no guia 01, seção 14.

### K8S-05 — RabbitMQ com operator
- **Tipo:** infra · **Fase:** 7 · **Depende de:** K8S-03
- **Contexto:** instale o RabbitMQ Cluster Operator e crie um `RabbitmqCluster` com uma réplica. Declare a topologia (exchange, filas e DLX) — o notification a declara sozinho na subida, mas prefira entender os dois caminhos e anote a decisão. Guarde em `k8s/rabbitmq/`.
- **Pronto quando:** o pod está `Ready`; o painel de gestão é acessível por `port-forward`; o `Secret` gerado pelo operator com usuário e senha é localizado e anotado para uso nos serviços.
- **Referência:** guia 01, seção 10.2 (passo 4).

### K8S-06 — Postgres
- **Tipo:** infra · **Fase:** 7 · **Depende de:** K8S-03
- **Contexto:** `StatefulSet` com `PersistentVolumeClaim`, `Service` headless/ClusterIP, `Secret` com a senha e um mecanismo para criar as 4 databases na primeira subida (script de init montado por `ConfigMap`). Fazer na mão ensina mais que usar um chart.
- **Pronto quando:** o pod persiste dados após ser deletado e recriado (teste criando uma tabela, deletando o pod e conferindo); as 4 databases existem.
- **Referência:** guia 01, seção 10.1 e 10.2 (passo 5).

---

## K8S-E2 — Aplicações (Fase 7)

Cada serviço com `ConfigMap`, `Secret`, `Deployment` (probes e resources) e `Service`, mais Ingress, Kustomize e HPA.

**Progresso:**
- [ ] K8S-07 order-service
- [ ] K8S-08 catalog-service
- [ ] K8S-09 payment-service
- [ ] K8S-10 notification-service
- [ ] K8S-11 shipping-service (web e worker)
- [ ] K8S-12 Ingress
- [ ] K8S-13 Fluxo completo pelo Ingress
- [ ] K8S-14 Kustomize
- [ ] K8S-15 metrics-server e HPA

### K8S-07 — order-service
- **Tipo:** infra · **Fase:** 7 · **Depende de:** K8S-04, K8S-05, K8S-06, ORD-34
- **Contexto:** carregue a imagem no kind (sem registry) e escreva os manifests em `k8s/order/`: `ConfigMap` (brokers, tópicos), `Secret` (banco e RabbitMQ), `Deployment` com `imagePullPolicy: IfNotPresent`, `readinessProbe`, `livenessProbe`, `requests` e `limits`, e `Service` ClusterIP. Aplique a migration do banco de algum jeito explícito (job ou passo manual) e anote qual.
- **Pronto quando:** o pod fica `Ready`; `kubectl port-forward` permite `POST /orders` e `GET /orders/:id`; a mensagem `order.created` aparece no tópico.
- **Referência:** guia 01, seção 10.2 (passo 6).

### K8S-08 — catalog-service
- **Tipo:** infra · **Fase:** 7 · **Depende de:** K8S-04, K8S-06, CAT-26
- **Contexto:** mesma estrutura em `k8s/catalog/`. Atenção ao consumer group: réplicas dividem as partições.
- **Pronto quando:** pod `Ready`; cadastro de produto via `port-forward`; ao receber `order.created` o estoque é reservado.
- **Referência:** guia 01, seção 10.2 (passo 6).

### K8S-09 — payment-service
- **Tipo:** infra · **Fase:** 7 · **Depende de:** K8S-04, K8S-06, PAY-19
- **Contexto:** mesma estrutura em `k8s/payment/`. O health check usa o endpoint do Actuator e a JVM precisa de `requests`/`limits` coerentes (o startup é lento: ajuste o `initialDelay` ou use `startupProbe`).
- **Pronto quando:** pod `Ready` sem reinícios por probe; `GET /payments?orderId=` responde pelo `port-forward`.
- **Referência:** guia 01, seção 10.1 (probes).

### K8S-10 — notification-service
- **Tipo:** infra · **Fase:** 7 · **Depende de:** K8S-05, NOT-14
- **Contexto:** em `k8s/notification/`, sem banco. Configure a URL do RabbitMQ a partir do `Secret` do operator. Garanta que `terminationGracePeriodSeconds` seja suficiente para o desligamento gracioso.
- **Pronto quando:** pod `Ready`; o consumidor aparece na fila `notifications.email`; ao deletar o pod, o log mostra o shutdown gracioso.
- **Referência:** guia 01, seção 12 (fase 4, graceful shutdown).

### K8S-11 — shipping-service (web e worker)
- **Tipo:** infra · **Fase:** 7 · **Depende de:** K8S-04, K8S-05, K8S-06, SHP-26
- **Contexto:** mesma imagem em **dois Deployments** em `k8s/shipping/`: `shipping-web` (API, com probe HTTP e `Service`) e `shipping-worker` (comando `shipping:consume-orders`, sem `Service`, com probe adequada a um processo sem HTTP). Escalam de forma independente.
- **Pronto quando:** os dois ficam `Ready`; o worker aparece no consumer group; `order.confirmed` cria o `Shipment` e o `dispatch` pelo `port-forward` publica `shipment.dispatched`.
- **Referência:** guia 01, seção 10.2 (passo 6, nota sobre o shipping).

### K8S-12 — Ingress
- **Tipo:** infra · **Fase:** 7 · **Depende de:** K8S-02, K8S-07, K8S-08, K8S-09, K8S-11
- **Contexto:** um único `Ingress` roteando por path: `/orders` → order, `/products` → catalog, `/payments` → payment, `/shipments` → shipping-web.
- **Pronto quando:** `curl localhost/orders/...`, `/products`, `/payments` e `/shipments` chegam em cada serviço certo; um path desconhecido devolve 404 do nginx.
- **Referência:** guia 01, seção 10.2 (passo 7).

### K8S-13 — Fluxo completo pelo Ingress
- **Tipo:** exercise · **Fase:** 7 · **Depende de:** K8S-12, K8S-10
- **Contexto:** adapte o `requests.http` para o host do Ingress e rode todos os cenários no cluster.
- **Pronto quando:** **critério de pronto da fase 7:** o fluxo completo funciona chamando o Ingress — pedido de R$ 100 `CONFIRMED` com e-mail e shipment, pedido de R$ 2000 `CANCELLED` com estoque devolvido, `@fail.com` na DLQ.
- **Referência:** guia 01, seção 12 (fase 7).

### K8S-14 — Kustomize
- **Tipo:** infra · **Fase:** 7 · **Depende de:** K8S-12
- **Contexto:** crie um `kustomization.yaml` na base que agrega os manifests de apps e Ingress, para que `kubectl apply -k k8s/` aplique tudo. Separe o que é plataforma (será Terraform na fase 8) do que é app, para não precisar mudar a estrutura depois. Anote o que o Kustomize resolve em relação a copiar YAML.
- **Pronto quando:** num cluster com a plataforma pronta, `kubectl apply -k k8s/` sobe todos os apps; um `kubectl kustomize` mostra o resultado esperado sem erro.
- **Referência:** guia 01, seções 7.6 e 12 (fase 9, Kustomize como extra).

### K8S-15 — metrics-server e HPA
- **Tipo:** infra · **Fase:** 7 · **Depende de:** K8S-07, K8S-03
- **Contexto:** instale o `metrics-server` (no kind é preciso permitir TLS inseguro no kubelet) e crie um HPA para o order-service escalando por CPU. Rode um teste de carga com `k6` ou `hey` contra `POST /orders`.
- **Pronto quando:** `kubectl top pods` funciona; sob carga o número de réplicas sobe até o máximo configurado e, depois de parar, volta ao mínimo.
- **Referência:** guia 01, seção 10.2 (passo 8) e 10.3.

---

## K8S-E3 — Exercícios de operação (Fase 7)

**Progresso:**
- [ ] K8S-16 Escalar o catalog e partições
- [ ] K8S-17 Resiliência
- [ ] K8S-18 Mensageria sob falha

### K8S-16 — Escalar o catalog e partições
- **Tipo:** exercise · **Fase:** 7 · **Depende de:** K8S-13
- **Contexto:** escale o catalog para 3 réplicas e depois 4 e observe, na Kafka UI (ou nos comandos do Kafka), como as 3 partições de `order.created` se distribuem no group `catalog-service`.
- **Pronto quando:** você mostra a atribuição de partições com 3 réplicas e explica por que a quarta fica ociosa.
- **Referência:** guia 01, seção 10.3 (primeiro item).

### K8S-17 — Resiliência
- **Tipo:** exercise · **Fase:** 7 · **Depende de:** K8S-13
- **Contexto:** mate um pod e veja o Deployment recriá-lo; quebre a readiness probe de um serviço e veja o pod sair do `Service`; faça um rolling update trocando a imagem (nova tag) sem derrubar o fluxo.
- **Pronto quando:** durante o rolling update o `requests.http` continua respondendo; você descreve o que a readiness e a liveness probe fazem de diferente.
- **Referência:** guia 01, seção 10.3.

### K8S-18 — Mensageria sob falha
- **Tipo:** exercise · **Fase:** 7 · **Depende de:** K8S-13
- **Contexto:** derrube o notification-service, gere pedidos e suba de novo; depois faça o mesmo com o `shipping-worker`. Compare o comportamento da fila do RabbitMQ (mensagens esperando) com o do Kafka (offset do consumer group).
- **Pronto quando:** você mostra que as mensagens esperaram nos dois casos e explica a diferença entre fila e log.
- **Referência:** guia 01, seção 10.3 (último item) e seção 1 (Kafka × RabbitMQ).
