# TF — Terraform (infra como código)

Reescrever a **plataforma** do Kubernetes (cluster kind, operators, tópicos, filas, Postgres) como código Terraform, mantendo os **apps** em YAML/Kustomize em `k8s/`. Tudo local, sem custo. Fase 8.

Referências gerais: guia 01, seção 11 (Terraform) e seção 12 (fase 8); guia 02, seção 6 (HCL).

Pré-requisito: a fase 7 fechada. Você só consegue escrever o código de forma consciente depois de ter feito a versão manual.

---

## TF-E1 — Fundação e cluster (Fase 8)

**Progresso:**
- [ ] TF-01 Estrutura do projeto Terraform
- [ ] TF-02 Módulo `cluster`
- [ ] TF-03 Ingress, metrics-server e namespace
- [ ] TF-04 Operators do Kafka e do RabbitMQ

### TF-01 — Estrutura do projeto Terraform
- **Tipo:** setup · **Fase:** 8 · **Depende de:** K8S-14, INF-01
- **Contexto:** crie `terraform/` com `versions.tf` (versão do Terraform e providers `tehcyx/kind`, `hashicorp/helm` e `hashicorp/kubernetes` com versões travadas), `variables.tf`, `outputs.tf` e a chamada dos módulos em `main.tf`. Atualize o `.gitignore` para state, `.terraform/` e `*.tfvars` com segredos.
- **Pronto quando:** `terraform init`, `terraform fmt -check` e `terraform validate` passam; `git status` não mostra state nem diretório de providers.
- **Referência:** guia 01, seção 11.4; guia 02, seção 6 (arquivo principal e comandos).

### TF-02 — Módulo `cluster`
- **Tipo:** infra · **Fase:** 8 · **Depende de:** TF-01
- **Contexto:** `modules/cluster` cria o cluster kind equivalente ao da K8S-01 (portas 80/443, label do ingress) com o nome vindo de variável. Exporte o caminho do kubeconfig como output para configurar os providers do Helm e do Kubernetes.
- **Pronto quando:** `terraform apply` cria o cluster; `terraform destroy` o remove; `kubectl get nodes` funciona com o kubeconfig do output.
- **Referência:** guia 01, seção 11.3; guia 02, seção 6 (módulos e outputs).

### TF-03 — Ingress, metrics-server e namespace
- **Tipo:** infra · **Fase:** 8 · **Depende de:** TF-02
- **Contexto:** crie o namespace `shopflow` com `kubernetes_namespace` e instale ingress-nginx e metrics-server com `helm_release`. Ajuste os valores do chart para o kind (nó de ingress, TLS do kubelet).
- **Pronto quando:** num apply do zero, os pods de ingress e metrics-server ficam `Running`; `kubectl top nodes` funciona; o plan seguinte não mostra mudanças.
- **Referência:** guia 01, seção 11.3; guia 02, seção 6 (Helm e Kubernetes).

### TF-04 — Operators do Kafka e do RabbitMQ
- **Tipo:** infra · **Fase:** 8 · **Depende de:** TF-03
- **Contexto:** `helm_release` do Strimzi, do RabbitMQ Cluster Operator e do Messaging Topology Operator (necessário para declarar exchanges e filas como recursos). Use `wait` para só considerar pronto quando os pods subirem.
- **Pronto quando:** as CRDs `Kafka`, `KafkaTopic`, `RabbitmqCluster`, `Exchange`, `Queue` e `Binding` existem no cluster (`kubectl get crd`); novo plan sem diferenças.
- **Referência:** guia 01, seção 11.3.

---

## TF-E2 — Mensageria e dados (Fase 8)

**Progresso:**
- [ ] TF-05 Resolver CRDs no plan
- [ ] TF-06 Kafka em KRaft
- [ ] TF-07 Tópicos com `for_each`
- [ ] TF-08 RabbitMQ e topologia
- [ ] TF-09 Módulo `database`

### TF-05 — Resolver CRDs no plan
- **Tipo:** infra · **Fase:** 8 · **Depende de:** TF-04
- **Contexto:** `kubernetes_manifest` precisa que a CRD já exista **durante o plan**, o que falha se o operator for instalado no mesmo apply. Escolha uma saída — dois estágios com `-target`, duas pastas/states (por exemplo, `platform` e `messaging`) ou outro provider de manifests — e registre a decisão e o motivo no README do Terraform.
- **Pronto quando:** você consegue subir tudo do zero seguindo os passos documentados, sem erro de CRD; consegue explicar por que o erro acontece.
- **Referência:** guia 01, seção 11.3 (pegadinha clássica).

### TF-06 — Kafka em KRaft
- **Tipo:** infra · **Fase:** 8 · **Depende de:** TF-05
- **Contexto:** no módulo `messaging`, declare o recurso `Kafka` (KRaft) e o node pool correspondente com `kubernetes_manifest`, equivalente ao que você fez na K8S-04. Parametrize nome do cluster e namespace.
- **Pronto quando:** o broker fica `Ready` após o apply; o endereço de bootstrap é exposto como output.
- **Referência:** guia 01, seção 11.3.

### TF-07 — Tópicos com `for_each`
- **Tipo:** infra · **Fase:** 8 · **Depende de:** TF-06
- **Contexto:** uma lista única `locals.topics` com os 8 tópicos da seção 3.2 e um `KafkaTopic` por item via `for_each` (nomes do recurso k8s derivados do nome do tópico, já que `.` pode não ser aceito). Partições e réplicas como variáveis com padrão 3 e 1.
- **Pronto quando:** `terraform apply` cria os 8 `KafkaTopic`; adicionar um nono item à lista e rodar o plan mostra exatamente 1 recurso a criar.
- **Referência:** guia 01, seções 3.2 e 11.3; guia 02, seção 6 (`for_each`).

### TF-08 — RabbitMQ e topologia
- **Tipo:** infra · **Fase:** 8 · **Depende de:** TF-05
- **Contexto:** `RabbitmqCluster` e, com o Messaging Topology Operator, `Exchange` (`notifications` e `notifications.dlx`), `Queue` (`notifications.email` com argumento de dead-letter e `notifications.email.dlq`) e `Binding`. Os parâmetros precisam bater com o que o notification declara na subida, senão o broker recusa.
- **Pronto quando:** o painel mostra a topologia completa criada pelo Terraform; o notification sobe sem erro de "parâmetros diferentes".
- **Referência:** guia 01, seções 3.3 e 11.3.

### TF-09 — Módulo `database`
- **Tipo:** infra · **Fase:** 8 · **Depende de:** TF-03
- **Contexto:** Postgres como `StatefulSet` com PVC e `Service`, `Secret` com a senha vinda de uma variável `sensitive` (valor fora do git) e a criação das 4 databases. Entenda que `sensitive` esconde o valor do plan e do output, mas **ele ainda vai para o state**.
- **Pronto quando:** num apply do zero, o Postgres sobe com as 4 databases; a senha não aparece no plan; você consegue dizer onde o segredo ainda está exposto.
- **Referência:** guia 01, seções 11.2 e 11.3.

---

## TF-E3 — Qualidade e validação (Fase 8)

**Progresso:**
- [ ] TF-10 Teste dos tópicos
- [ ] TF-11 Teste da DLX
- [ ] TF-12 Recriação do zero sem drift
- [ ] TF-13 Exercícios de Terraform

### TF-10 — Teste dos tópicos
- **Tipo:** test-integration · **Fase:** 8 · **Depende de:** TF-07
- **Contexto:** escreva `tests/messaging.tftest.hcl` com `mock_provider` e `command = plan`: com uma lista de tópicos de entrada, o número de `KafkaTopic` planejados é igual ao tamanho da lista, e partições e réplicas seguem as variáveis.
- **Pronto quando:** `terraform test` passa; remover um tópico da lista no teste muda o resultado esperado; quebrar propositalmente a lógica do `for_each` faz o teste falhar.
- **Referência:** guia 01, seções 8 e 11; guia 02, seção 6 (testes).

### TF-11 — Teste da DLX
- **Tipo:** test-integration · **Fase:** 8 · **Depende de:** TF-08
- **Contexto:** outro run de teste verificando que a fila `notifications.email` declara `x-dead-letter-exchange` apontando para `notifications.dlx` e que existe o binding da DLQ.
- **Pronto quando:** `terraform test` passa; remover o argumento de dead-letter do módulo faz o teste falhar.
- **Referência:** guia 01, seção 8 (terraform).

### TF-12 — Recriação do zero sem drift
- **Tipo:** exercise · **Fase:** 8 · **Depende de:** TF-04, TF-07, TF-08, TF-09, K8S-13
- **Contexto:** destrua o cluster manual da fase 7, rode o Terraform do zero (seguindo a decisão da TF-05), carregue as imagens no kind, aplique `kubectl apply -k k8s/` e rode o fluxo completo.
- **Pronto quando:** **critério de pronto da fase 8:** `terraform apply` num ambiente limpo sobe cluster, operators, tópicos, filas e Postgres; `kubectl apply -k k8s/` deixa o fluxo completo funcionando; um `terraform plan` imediatamente depois não mostra mudanças.
- **Referência:** guia 01, seção 12 (fase 8).

### TF-13 — Exercícios de Terraform
- **Tipo:** exercise · **Fase:** 8 · **Depende de:** TF-12
- **Contexto:** faça os exercícios da seção 11.5: `destroy` + `apply` cronometrado; provocar **drift** apagando um `KafkaTopic` com `kubectl` e lendo o plan; adicionar um tópico só pela lista; reaproveitar o módulo `messaging` com outro nome/namespace; inspecionar o state com `terraform state list` e `show`.
- **Pronto quando:** você registra, em poucas linhas cada, o que o plan propôs no drift, por que o state não pode ir para o git e como o mesmo módulo foi reaproveitado.
- **Referência:** guia 01, seção 11.5.
