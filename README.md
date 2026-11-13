# ShopFlow

Projeto de estudo de microserviços poliglotas. Arquitetura e roadmap: [01-arquitetura-e-roadmap.md](01-arquitetura-e-roadmap.md). Tarefas: [backlog/](backlog/README.md).

## Convenção de commits

[Conventional Commits](https://www.conventionalcommits.org/pt-br/): `tipo(escopo): descrição curta no imperativo`.

**Tipos**

| Tipo | Quando usar |
|---|---|
| `feat` | funcionalidade nova |
| `fix` | correção de bug |
| `test` | só testes (criar ou ajustar) |
| `refactor` | muda o código sem mudar o comportamento |
| `docs` | só documentação |
| `chore` | tarefa de manutenção (dependências, `.gitignore`) |

**Escopo** = o serviço ou a área: `order`, `catalog`, `payment`, `notification`, `shipping`, `infra`, `k8s`, `terraform`.

**Exemplos**

```
feat(order): cria o value object Money
test(catalog): cobre reserva de estoque insuficiente
fix(payment): evita processar o mesmo eventId duas vezes
feat(shipping): adiciona o consumer de order.confirmed
feat(infra): sobe Postgres com as 4 databases
chore: ignora state e tfvars do Terraform
```

**Regras**
- Um commit por mudança lógica; no TDD, o teste e o código que o faz passar podem ir juntos.
- Descrição em até ~70 caracteres, sem ponto final.
- Mudança que quebra contrato (envelope, tópico, formato de fila): `feat(order)!: ...`.
- Branch por issue: `feat/INF-01-preparar-repositorio`. O PR leva `Closes #<número da issue>`.
