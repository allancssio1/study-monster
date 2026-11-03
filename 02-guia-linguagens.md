# Guia de Referência — TypeScript/NestJS · Python/FastAPI · Java/Spring · Go

> Consulta rápida para o projeto ShopFlow. As quatro seções têm **a mesma ordem de tópicos** para facilitar a comparação.
> Os exemplos usam o domínio do projeto, mas são **trechos de sintaxe**, não a implementação.
>
> Arquitetura e roadmap: [`01-arquitetura-e-roadmap.md`](./01-arquitetura-e-roadmap.md)

## Sumário

- [Tabela comparativa](#tabela-comparativa)
- [1. TypeScript + NestJS](#1-typescript--nestjs)
- [2. Python + FastAPI](#2-python--fastapi)
- [3. Java + Spring Boot](#3-java--spring-boot)
- [4. Go](#4-go)

Tópicos de cada seção: arquivo principal · ferramentas e comandos · variáveis e constantes · tipos · funções · classes e constructor · interfaces e herança · erros · async/concorrência · null-safety · módulos/imports · decorators/annotations · injeção de dependência · testes · Kafka e RabbitMQ.

---

## Tabela comparativa

| Tópico | TypeScript / NestJS | Python / FastAPI | Java / Spring | Go |
|---|---|---|---|---|
| Arquivo principal | `src/main.ts` | `app/main.py` | `PaymentApplication.java` | `cmd/<app>/main.go` |
| Gerenciador | npm / pnpm (`package.json`) | uv / pip (`pyproject.toml`) | Maven / Gradle (`pom.xml`) | go modules (`go.mod`) |
| Rodar | `npm run start:dev` | `uv run fastapi dev app/main.py` | `./mvnw spring-boot:run` | `go run ./cmd/notification` |
| Testar | `npm test` (Jest) | `uv run pytest` | `./mvnw test` (JUnit 5) | `go test ./...` |
| Tipagem | Estática, estrutural, opcional em runtime | Dinâmica + type hints (checados por mypy/pyright) | Estática, nominal | Estática, estrutural nas interfaces |
| Variável | `let x = 1` | `x = 1` | `int x = 1;` / `var x = 1;` | `x := 1` / `var x int = 1` |
| Constante | `const X = 1` | `X: Final = 1` (convenção) | `static final int X = 1;` | `const X = 1` |
| Função | `function f(a: number): number {}` | `def f(a: int) -> int:` | `int f(int a) {}` (dentro de classe) | `func f(a int) int {}` |
| Lambda | `(a) => a * 2` | `lambda a: a * 2` | `a -> a * 2` | `func(a int) int { return a * 2 }` |
| Classe | `class Order {}` | `class Order:` | `class Order {}` | não tem → `type Order struct {}` |
| Constructor | `constructor(...) {}` | `def __init__(self, ...):` | `Order(...) {}` | convenção `func NewOrder(...) (*Order, error)` |
| `this` | `this` | `self` (explícito) | `this` | receiver: `func (o *Order) Confirm()` |
| Privado | `private` / `#campo` | `_campo` (convenção) | `private` | letra minúscula (privado ao pacote) |
| Interface | `interface` (estrutural) | `ABC` ou `Protocol` | `interface` (+ `implements`) | `interface` (implícita) |
| Herança | `extends` | `class B(A):` | `extends` | não tem → composição (embedding) |
| Imutável | `readonly` | `@dataclass(frozen=True)` | `final` / `record` | não tem; use campos privados + sem setters |
| Enum | union de literais ou `enum` | `class S(Enum):` | `enum S {}` | `const ( A = iota ... )` |
| Nulo | `null` / `undefined` | `None` | `null` / `Optional<T>` | `nil` |
| Erros | `throw` / `try-catch` | `raise` / `try-except` | `throw` / `try-catch` | retorno `error` + `if err != nil` |
| Async | `async/await` + Promise | `async/await` + asyncio | Threads / virtual threads / `CompletableFuture` | goroutines + channels |
| Metadados | Decorators `@Controller()` | Decorators `@router.get()` | Annotations `@RestController` | Struct tags `` `json:"id"` `` |
| DI | Container do Nest (`providers`) | `Depends()` | Container do Spring (beans) | Manual no `main.go` |
| Dinheiro | lib decimal ou centavos em `number` inteiro | `Decimal` | `BigDecimal` | centavos em `int64` |
| Kafka | `@nestjs/microservices` + `kafkajs` | `aiokafka` | `spring-kafka` | `segmentio/kafka-go` |
| RabbitMQ | `amqplib` | `aio-pika` | `spring-boot-starter-amqp` | `rabbitmq/amqp091-go` |

---

## 1. TypeScript + NestJS

### Arquivo principal

```ts
// src/main.ts
import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));

  // consumidor Kafka no mesmo processo (app híbrido)
  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: { brokers: [process.env.KAFKA_BROKERS ?? 'localhost:9092'] },
      consumer: { groupId: 'order-service' },
    },
  });

  await app.startAllMicroservices();
  await app.listen(process.env.PORT ?? 3000);
}
bootstrap();
```

```ts
// src/app.module.ts
@Module({ imports: [OrdersModule] })
export class AppModule {}
```

### Ferramentas e comandos

| Ação | Comando |
|---|---|
| Criar projeto | `npx @nestjs/cli new order-service` |
| Gerar peças | `nest g module orders` · `nest g controller orders` |
| Adicionar dep | `npm i <pkg>` / `pnpm add <pkg>` · dev: `-D` |
| Rodar (watch) | `npm run start:dev` |
| Testes | `npm test` · `npm run test:watch` · `npm run test:e2e` · `npm run test:cov` |
| Build | `npm run build` → `dist/` |

### Variáveis e constantes

```ts
const MAX_ITEMS = 50;          // não pode reatribuir (prefira sempre)
let attempts = 0;              // pode reatribuir
// var — não use (escopo de função, hoisting)

const ids = ['a', 'b'];
ids.push('c');                 // OK: const impede reatribuir, não mutar
```

### Tipos

```ts
// primitivos
let name: string = 'Ana';
let price: number = 10.5;      // number = float64 (cuidado com dinheiro!)
let active: boolean = true;
let big: bigint = 10n;
let nothing: null = null;
let notSet: undefined;

// coleções
const ids: string[] = [];              // ou Array<string>
const pair: [string, number] = ['a', 1]; // tupla
const stock: Record<string, number> = { p1: 10 };
const map = new Map<string, number>();
const set = new Set<string>();

// union e literal (use no lugar de enum)
type OrderStatus = 'PENDING' | 'CONFIRMED' | 'CANCELLED';

// objeto: type alias ou interface
type CreateOrderInput = {
  customerId: string;
  items: { productId: string; quantity: number; unitPrice: number }[];
  note?: string;                // opcional
};

// especiais
let x: unknown;   // "não sei" — precisa checar antes de usar (prefira a any)
let y: any;       // desliga o type checker — evite
function fail(): never { throw new Error(); }

// generics
function first<T>(list: T[]): T | undefined { return list[0]; }

// utilitários
type Partial_ = Partial<CreateOrderInput>;
type OnlyId = Pick<CreateOrderInput, 'customerId'>;
type Ro = Readonly<CreateOrderInput>;
```

### Funções

```ts
function total(a: number, b: number): number { return a + b; }

const double = (n: number): number => n * 2;     // arrow function

function greet(name: string, greeting = 'Olá', suffix?: string): string {
  return `${greeting}, ${name}${suffix ?? ''}`;
}

function sum(...values: number[]) { return values.reduce((a, v) => a + v, 0); }

// desestruturação nos parâmetros
function create({ customerId, items }: CreateOrderInput) {}
```

### Classes e constructor

```ts
export class Order {
  private status: OrderStatus = 'PENDING';
  private readonly items: OrderItem[];
  static readonly MAX_ITEMS = 50;

  // constructor privado + factory estática = força passar pelas validações
  private constructor(
    public readonly id: string,          // "parameter property": declara e atribui
    private readonly customerId: string,
    items: OrderItem[],
  ) {
    this.items = items;
  }

  static create(id: string, customerId: string, items: OrderItem[]): Order {
    if (items.length === 0) throw new DomainError('Order must have items');
    return new Order(id, customerId, items);
  }

  get currentStatus(): OrderStatus { return this.status; }   // getter

  confirm(): void {
    if (this.status !== 'PENDING') throw new DomainError('Only pending orders can be confirmed');
    this.status = 'CONFIRMED';
  }
}

// modificadores: public (padrão) · private · protected · readonly · static · abstract
// #campo = privado de verdade em runtime (private do TS some no JS)
```

### Interfaces, classes abstratas e herança

```ts
interface EventPublisher {
  publish(events: DomainEvent[]): Promise<void>;
}

class KafkaEventPublisher implements EventPublisher {
  async publish(events: DomainEvent[]) { /* ... */ }
}

abstract class AggregateRoot {
  private events: DomainEvent[] = [];
  protected addEvent(e: DomainEvent) { this.events.push(e); }
  pullEvents(): DomainEvent[] { const e = this.events; this.events = []; return e; }
}

class Order extends AggregateRoot {
  constructor() { super(); }     // super() obrigatório antes de usar this
}
```

> **Importante no Nest:** interfaces **somem** no JavaScript compilado, então não servem como token de DI.
> Para ports e repositórios use **`abstract class`** (existe em runtime) ou um token `Symbol`/string com `@Inject()`.

### Erros

```ts
export class DomainError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DomainError';
  }
}
export class OrderNotFoundError extends DomainError {}

try {
  order.confirm();
} catch (err) {
  if (err instanceof DomainError) { /* ... */ }
  throw err;
} finally { /* sempre roda */ }
```

No Nest, traduza erros de domínio para HTTP com um **Exception Filter** (ou lance `NotFoundException`, `ConflictException`, `BadRequestException` no controller):

```ts
@Catch(OrderNotFoundError)
export class OrderNotFoundFilter implements ExceptionFilter {
  catch(exception: OrderNotFoundError, host: ArgumentsHost) {
    const res = host.switchToHttp().getResponse();
    res.status(404).json({ error: 'ORDER_NOT_FOUND', message: exception.message });
  }
}
```

### Async

```ts
async function load(id: string): Promise<Order | null> {
  const order = await repo.findById(id);   // await só dentro de async
  return order;
}

const [a, b] = await Promise.all([repo.findById('1'), repo.findById('2')]); // paralelo
```

Node é single-thread com event loop: `await` libera a thread enquanto espera I/O.

### Null-safety

```ts
// tsconfig: "strict": true  (ativa strictNullChecks)
const email = order?.customer?.email;      // optional chaining
const port = process.env.PORT ?? 3000;     // nullish coalescing (só null/undefined)
const id = maybeId!;                       // non-null assertion — evite
```

### Módulos / imports

```ts
export class Order {}                      // named export (prefira)
export default class X {}                  // default export
import { Order } from './order.entity';
import * as crypto from 'node:crypto';
```

### Decorators do NestJS

| Decorator | Onde | Para quê |
|---|---|---|
| `@Module({ imports, controllers, providers, exports })` | classe | Agrupa uma feature; define o que é injetável |
| `@Controller('orders')` | classe | Controller HTTP com prefixo de rota |
| `@Get()` `@Post()` `@Put()` `@Patch()` `@Delete()` | método | Rota HTTP |
| `@Param('id')` `@Body()` `@Query()` `@Headers()` | parâmetro | Extrai partes do request |
| `@HttpCode(201)` | método | Status de resposta |
| `@Injectable()` | classe | Pode ser injetada / receber injeção |
| `@Inject(TOKEN)` | parâmetro do constructor | Injeta por token (string/Symbol) |
| `@EventPattern('payment.approved')` | método | Consome evento (Kafka/RMQ) sem resposta |
| `@MessagePattern('topic')` | método | Request-response via broker |
| `@Payload()` `@Ctx()` | parâmetro | Corpo e contexto da mensagem |
| `@UseFilters()` `@UsePipes()` `@UseGuards()` `@UseInterceptors()` | classe/método | Pipeline do request |
| `@Catch(Erro)` | classe | Exception filter |
| `@IsString()` `@IsInt()` `@Min(1)` `@IsEmail()` `@ValidateNested()` `@Type(() => X)` | propriedade de DTO | Validação (libs `class-validator` / `class-transformer`) |

```ts
// presentation/controllers/create-order.request.ts
export class OrderItemRequest {
  @IsString() productId: string;
  @IsInt() @Min(1) quantity: number;
  @IsPositive() unitPrice: number;
}
export class CreateOrderRequest {
  @IsString() customerId: string;
  @IsEmail() customerEmail: string;
  @ValidateNested({ each: true }) @Type(() => OrderItemRequest)
  items: OrderItemRequest[];
}

// presentation/controllers/orders.controller.ts
@Controller('orders')
export class OrdersController {
  constructor(private readonly createOrder: CreateOrderUseCase) {}

  @Post()
  @HttpCode(201)
  async create(@Body() body: CreateOrderRequest) {
    return this.createOrder.execute(body);
  }

  @Get(':id')
  async get(@Param('id') id: string) { /* ... */ }
}

// presentation/consumers/payment-events.consumer.ts
@Controller()
export class PaymentEventsConsumer {
  @EventPattern('payment.approved')
  async onApproved(@Payload() event: PaymentApprovedEvent) { /* ... */ }
}
```

### Injeção de dependência

```ts
// domain/repositories/order.repository.ts
export abstract class OrderRepository {
  abstract save(order: Order): Promise<void>;
  abstract findById(id: string): Promise<Order | null>;
}

// orders.module.ts — liga port → adapter
@Module({
  controllers: [OrdersController, PaymentEventsConsumer],
  providers: [
    { provide: OrderRepository, useClass: PostgresOrderRepository },
    { provide: EventPublisher, useClass: KafkaEventPublisher },
    CreateOrderUseCase,
  ],
})
export class OrdersModule {}

// use case recebe a abstração
@Injectable()
export class CreateOrderUseCase {
  constructor(
    private readonly orders: OrderRepository,
    private readonly publisher: EventPublisher,
  ) {}
  async execute(input: CreateOrderInput): Promise<OrderOutput> { /* ... */ }
}
```

> Quer o use case **sem nenhum import do Nest** (Clean Arch mais pura)? Não use `@Injectable` nele e registre com `useFactory`:
> `{ provide: CreateOrderUseCase, useFactory: (r, p) => new CreateOrderUseCase(r, p), inject: [OrderRepository, EventPublisher] }`

### Testes (Jest — padrão do Nest)

```ts
// test/unit/create-order.usecase.spec.ts
describe('CreateOrderUseCase', () => {
  let repo: InMemoryOrderRepository;
  let publisher: { publish: jest.Mock };
  let sut: CreateOrderUseCase;            // sut = system under test

  beforeEach(() => {
    repo = new InMemoryOrderRepository();
    publisher = { publish: jest.fn() };
    sut = new CreateOrderUseCase(repo, publisher);
  });

  it('saves the order and publishes order.created', async () => {
    const out = await sut.execute({ customerId: 'c1', items: [{ productId: 'p1', quantity: 2, unitPrice: 10 }] });

    expect(out.status).toBe('PENDING');
    expect(repo.items).toHaveLength(1);
    expect(publisher.publish).toHaveBeenCalledTimes(1);
  });

  it('rejects an order without items', async () => {
    await expect(sut.execute({ customerId: 'c1', items: [] })).rejects.toThrow(DomainError);
  });
});
```

E2E: `Test.createTestingModule({ imports: [AppModule] }).overrideProvider(EventPublisher).useValue(fake).compile()` + `supertest` (`request(app.getHttpServer()).post('/orders').send(body).expect(201)`).

### Kafka e RabbitMQ

```ts
// Kafka — produtor via ClientKafka (registrado com ClientsModule.register no módulo)
constructor(@Inject('KAFKA') private readonly kafka: ClientKafka) {}
await lastValueFrom(this.kafka.emit('order.created', { key: order.id, value: JSON.stringify(envelope) }));

// RabbitMQ — amqplib direto (formato de mensagem livre, compatível com o consumer Go)
import * as amqp from 'amqplib';
const conn = await amqp.connect(process.env.RABBITMQ_URL!);
const ch = await conn.createConfirmChannel();
await ch.assertExchange('notifications', 'direct', { durable: true });
ch.publish('notifications', 'email.send', Buffer.from(JSON.stringify(msg)), { persistent: true, contentType: 'application/json' });
await ch.waitForConfirms();
```

> O `Transport.RMQ` do Nest envolve a mensagem em `{ pattern, data }`. Como o consumidor é Go, usar `amqplib` direto deixa o contrato limpo.

---

## 2. Python + FastAPI

### Arquivo principal

```python
# app/main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.modules.catalog.presentation.controllers.product_router import router as product_router
from app.modules.catalog.presentation.consumers.order_events_consumer import OrderEventsConsumer

@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer = OrderEventsConsumer(...)
    await consumer.start()          # roda antes de aceitar requests
    yield
    await consumer.stop()           # roda no shutdown

app = FastAPI(title="catalog-service", lifespan=lifespan)
app.include_router(product_router)

@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
```

### Ferramentas e comandos

| Ação | Comando |
|---|---|
| Criar projeto | `uv init catalog-service` |
| Adicionar dep | `uv add fastapi[standard] sqlalchemy asyncpg aiokafka pydantic-settings` |
| Dep de dev | `uv add --dev pytest pytest-asyncio httpx testcontainers mypy ruff` |
| Rodar (reload) | `uv run fastapi dev app/main.py` |
| Rodar (prod) | `uv run fastapi run app/main.py` ou `uvicorn app.main:app --host 0.0.0.0` |
| Testes | `uv run pytest` · `uv run pytest -k reserve -v` |
| Lint / tipos | `uv run ruff check .` · `uv run mypy app` |
| Migrations | `alembic init` · `alembic revision --autogenerate -m "..."` · `alembic upgrade head` |

### Variáveis e constantes

```python
max_items = 50                 # sem declaração; tipo vem do valor
attempts: int = 0              # type hint (não é checado em runtime)

from typing import Final
MAX_ITEMS: Final = 50          # constante por convenção (MAIÚSCULO) + Final p/ mypy
```

### Tipos

```python
# primitivos
name: str = "Ana"
qty: int = 2                   # inteiro de tamanho ilimitado
ratio: float = 0.5             # NÃO use para dinheiro
active: bool = True
nothing: None = None

from decimal import Decimal
price = Decimal("10.50")       # dinheiro: sempre Decimal criado a partir de string

# coleções (sintaxe moderna, Python 3.9+)
ids: list[str] = []
stock: dict[str, int] = {"p1": 10}
pair: tuple[str, int] = ("a", 1)
tags: set[str] = set()

# união e opcional (3.10+)
email: str | None = None

# literal / enum
from typing import Literal
Status = Literal["ACTIVE", "RELEASED"]

from enum import Enum
class ReservationStatus(str, Enum):
    ACTIVE = "ACTIVE"
    RELEASED = "RELEASED"

# generics (3.12+)
def first[T](items: list[T]) -> T | None:
    return items[0] if items else None

# alias de tipo
type ProductId = str
```

### Funções

```python
def total(a: int, b: int) -> int:
    return a + b

double = lambda n: n * 2                     # só para expressões curtas

def greet(name: str, greeting: str = "Olá") -> str:
    return f"{greeting}, {name}"

def log(*args: str, **kwargs: int) -> None:   # posicionais variáveis / nomeados variáveis
    ...

def create(*, name: str, price: Decimal) -> None:   # depois do * só aceita por nome
    ...

create(name="Caneca", price=Decimal("20"))
```

### Classes e constructor

```python
class Product:
    MAX_NAME = 100                              # atributo de classe

    def __init__(self, id: str, name: str, price: Decimal, stock: int) -> None:   # constructor
        if price <= 0:
            raise DomainError("price must be positive")
        self.id = id                            # público
        self._name = name                       # "protegido" (convenção _)
        self.__secret = 1                       # name mangling (raro)
        self._price = price
        self._stock = stock

    @property                                   # getter: product.stock
    def stock(self) -> int:
        return self._stock

    def reserve(self, qty: int) -> None:
        if qty > self._stock:
            raise InsufficientStock(self.id)
        self._stock -= qty

    @classmethod                                # factory: Product.create(...)
    def create(cls, name: str, price: Decimal, stock: int) -> "Product":
        return cls(str(uuid4()), name, price, stock)

    @staticmethod
    def normalize(name: str) -> str:
        return name.strip()

    def __eq__(self, other: object) -> bool:    # igualdade por identidade (entity)
        return isinstance(other, Product) and other.id == self.id
```

**Value Object com dataclass** (imutável, igualdade por valor automática):

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class Money:
    amount: Decimal

    def __post_init__(self) -> None:            # validação após o __init__ gerado
        if self.amount < 0:
            raise DomainError("money cannot be negative")

    def add(self, other: "Money") -> "Money":
        return Money(self.amount + other.amount)

Money(Decimal("10")) == Money(Decimal("10"))    # True
```

### Interfaces e herança

```python
from abc import ABC, abstractmethod

class ProductRepository(ABC):                   # "interface" nominal
    @abstractmethod
    async def save(self, product: Product) -> None: ...

    @abstractmethod
    async def find_by_id(self, id: str) -> Product | None: ...

class InMemoryProductRepository(ProductRepository):   # herança
    def __init__(self) -> None:
        self.items: dict[str, Product] = {}

    async def save(self, product: Product) -> None:
        self.items[product.id] = product

    async def find_by_id(self, id: str) -> Product | None:
        return self.items.get(id)

# alternativa estrutural (duck typing tipado): typing.Protocol — não precisa herdar
from typing import Protocol
class EventPublisher(Protocol):
    async def publish(self, events: list[DomainEvent]) -> None: ...

class Child(Base):
    def __init__(self) -> None:
        super().__init__()                      # chama o constructor da base
```

### Erros

```python
class DomainError(Exception):
    pass

class InsufficientStock(DomainError):
    def __init__(self, product_id: str) -> None:
        super().__init__(f"insufficient stock for {product_id}")
        self.product_id = product_id

try:
    product.reserve(5)
except InsufficientStock as e:
    ...
except (ValueError, TypeError):
    ...
else:
    ...                     # roda se não houve exceção
finally:
    ...                     # sempre roda
```

No FastAPI, traduza erro de domínio para HTTP:

```python
from fastapi import Request
from fastapi.responses import JSONResponse

@app.exception_handler(ProductNotFound)
async def not_found_handler(request: Request, exc: ProductNotFound) -> JSONResponse:
    return JSONResponse(status_code=404, content={"error": "PRODUCT_NOT_FOUND", "message": str(exc)})

# ou no router: raise HTTPException(status_code=404, detail="...")
```

### Async

```python
import asyncio

async def load(repo: ProductRepository, id: str) -> Product | None:
    return await repo.find_by_id(id)

a, b = await asyncio.gather(repo.find_by_id("1"), repo.find_by_id("2"))   # paralelo

task = asyncio.create_task(consumer.run())     # roda em background no event loop
```

> No FastAPI: `async def` para I/O com libs async (asyncpg, aiokafka). Se usar lib **síncrona** (ex.: psycopg2), declare a rota com `def` normal — o FastAPI roda num threadpool.

### Null-safety

```python
def find(id: str) -> Product | None: ...

product = find("1")
if product is None:          # compare None com "is"
    raise ProductNotFound(id)
product.reserve(1)           # mypy sabe que aqui não é None
```

### Módulos / imports

```python
# cada pasta com __init__.py é um pacote
from app.modules.catalog.domain.entities.product import Product      # import absoluto (prefira)
from .product import Product                                          # relativo
import json
```

### Decorators e o que o FastAPI usa

Decorator = função que recebe uma função/classe e devolve outra (`@x` em cima de `def f` equivale a `f = x(f)`).

| Decorator / recurso | Para quê |
|---|---|
| `@app.get("/path")` · `@router.post("/", status_code=201, response_model=X)` | Define rota |
| `APIRouter(prefix="/products", tags=["products"])` | Agrupa rotas (equivalente a um controller) |
| `Depends(fn)` | Injeção de dependência |
| `Path()` · `Query()` · `Body()` · `Header()` | Origem e validação de parâmetros |
| `BaseModel` (Pydantic) | Schema de request/response, validação e serialização |
| `Field(gt=0, max_length=100)` | Restrições de campo |
| `@field_validator("name")` · `@model_validator` | Validação customizada no Pydantic |
| `@asynccontextmanager` (lifespan) | Startup/shutdown |
| `@app.exception_handler(Erro)` | Converte exceção em resposta |
| `@dataclass` · `@property` · `@classmethod` · `@staticmethod` · `@abstractmethod` | Python puro |
| `@pytest.fixture` · `@pytest.mark.asyncio` · `@pytest.mark.parametrize` | Testes |

```python
# presentation/controllers/schemas.py
from pydantic import BaseModel, Field

class CreateProductRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    price: Decimal = Field(gt=0)
    stock: int = Field(ge=0)

class ProductResponse(BaseModel):
    id: str
    name: str
    price: Decimal
    stock: int

# presentation/controllers/product_router.py
from typing import Annotated
from fastapi import APIRouter, Depends

router = APIRouter(prefix="/products", tags=["products"])

@router.post("/", status_code=201, response_model=ProductResponse)
async def create_product(
    body: CreateProductRequest,
    use_case: Annotated[CreateProduct, Depends(get_create_product)],
) -> ProductResponse:
    output = await use_case.execute(body.name, body.price, body.stock)
    return ProductResponse(**output.__dict__)

@router.get("/{product_id}", response_model=ProductResponse)
async def get_product(product_id: str, use_case: Annotated[GetProduct, Depends(get_get_product)]):
    ...
```

### Injeção de dependência

```python
# modules/catalog/dependencies.py — composição das peças
async def get_session() -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        yield session                          # yield: fecha a sessão depois do request

def get_product_repository(session: Annotated[AsyncSession, Depends(get_session)]) -> ProductRepository:
    return SqlAlchemyProductRepository(session)

def get_create_product(repo: Annotated[ProductRepository, Depends(get_product_repository)]) -> CreateProduct:
    return CreateProduct(repo)                 # use case é Python puro, recebe pelo __init__
```

Nos testes: `app.dependency_overrides[get_product_repository] = lambda: InMemoryProductRepository()`.

### Testes (pytest)

```python
# tests/unit/test_product.py
import pytest

def test_reserve_decreases_stock():
    product = Product("p1", "Caneca", Decimal("20"), stock=10)
    product.reserve(3)
    assert product.stock == 7

def test_reserve_more_than_available_raises():
    product = Product("p1", "Caneca", Decimal("20"), stock=1)
    with pytest.raises(InsufficientStock):
        product.reserve(2)

@pytest.mark.parametrize("price", [Decimal("0"), Decimal("-1")])
def test_rejects_invalid_price(price):
    with pytest.raises(DomainError):
        Product("p1", "Caneca", price, stock=1)

# tests/conftest.py — fixtures reutilizáveis
@pytest.fixture
def repo() -> InMemoryProductRepository:
    return InMemoryProductRepository()

# use case async (pytest-asyncio; ou asyncio_mode = "auto" no pyproject)
@pytest.mark.asyncio
async def test_reserve_stock_publishes_event(repo):
    publisher = FakePublisher()
    await ReserveStock(repo, publisher).execute(order_created_event)
    assert publisher.published[0].event_type == "stock.reserved"

# e2e
from fastapi.testclient import TestClient
def test_create_product_api():
    client = TestClient(app)
    res = client.post("/products/", json={"name": "Caneca", "price": "20.00", "stock": 5})
    assert res.status_code == 201
```

### Kafka e RabbitMQ

```python
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
await producer.start()
await producer.send_and_wait("stock.reserved", key=order_id.encode(), value=json.dumps(envelope).encode())

consumer = AIOKafkaConsumer("order.created", "order.cancelled",
                            bootstrap_servers="localhost:9092",
                            group_id="catalog-service",
                            enable_auto_commit=False)
await consumer.start()
async for msg in consumer:
    await handle(msg.topic, json.loads(msg.value))
    await consumer.commit()               # commit só depois de processar

# RabbitMQ (se precisar): aio-pika
import aio_pika
conn = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
```

---

## 3. Java + Spring Boot

### Arquivo principal

```java
// src/main/java/com/shopflow/payment/PaymentApplication.java
package com.shopflow.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication   // = @Configuration + @EnableAutoConfiguration + @ComponentScan (deste pacote para baixo)
public class PaymentApplication {
    public static void main(String[] args) {
        SpringApplication.run(PaymentApplication.class, args);
    }
}
```

```yaml
# src/main/resources/application.yml
server:
  port: 8080
spring:
  datasource:
    url: ${DB_URL:jdbc:postgresql://localhost:5432/payments}
    username: ${DB_USER:postgres}
    password: ${DB_PASSWORD:postgres}
  jpa:
    hibernate.ddl-auto: validate     # use Flyway para migrations
  kafka:
    bootstrap-servers: ${KAFKA_BROKERS:localhost:9092}
    consumer:
      group-id: payment-service
      auto-offset-reset: earliest
  threads:
    virtual:
      enabled: true                  # Java 21: virtual threads
management:
  endpoints.web.exposure.include: health
```

### Ferramentas e comandos

| Ação | Comando |
|---|---|
| Criar projeto | https://start.spring.io (ou `spring init` da Spring CLI) |
| Rodar | `./mvnw spring-boot:run` |
| Testes | `./mvnw test` (unit, `*Test`) · `./mvnw verify` (inclui `*IT` com failsafe) |
| Build | `./mvnw package` → `target/*.jar` · `java -jar target/payment-service.jar` |
| Imagem sem Dockerfile | `./mvnw spring-boot:build-image` |
| Dependência | adicionar `<dependency>` no `pom.xml` |

### Variáveis e constantes

```java
int attempts = 0;
final int max = 50;                         // não pode reatribuir
var payments = new ArrayList<Payment>();    // inferência (só variável local)

public static final int MAX_ATTEMPTS = 3;   // constante de classe
```

### Tipos

```java
// primitivos (não podem ser null)
int qty = 1; long id = 1L; double ratio = 0.5; boolean ok = true; char c = 'a';

// wrappers (podem ser null; usados em generics)
Integer boxed = 1; Long l = 1L;

String name = "Ana";                        // imutável
BigDecimal amount = new BigDecimal("10.50");   // DINHEIRO: sempre BigDecimal a partir de String
amount.compareTo(BigDecimal.valueOf(1000)) > 0; // compare com compareTo, não equals

// coleções
List<String> ids = new ArrayList<>();
List<String> fixed = List.of("a", "b");     // imutável
Map<String, Integer> stock = new HashMap<>();
Set<String> tags = new HashSet<>();
Optional<Payment> maybe = Optional.empty();

// enum
public enum PaymentStatus { APPROVED, REJECTED }

// record = classe imutável com constructor, getters, equals/hashCode/toString gerados
public record Money(BigDecimal amount) {
    public Money {                          // "compact constructor" para validar
        if (amount.signum() < 0) throw new DomainException("negative money");
    }
    public Money add(Money other) { return new Money(amount.add(other.amount)); }
}

// generics
public <T> T first(List<T> list) { return list.isEmpty() ? null : list.get(0); }
```

### Funções (métodos), lambdas e streams

```java
// tudo vive dentro de uma classe
public int total(int a, int b) { return a + b; }
public static int sum(int... values) { return Arrays.stream(values).sum(); }   // varargs

// sobrecarga (mesmo nome, parâmetros diferentes)
public Money of(String v) { ... }
public Money of(BigDecimal v) { ... }

// lambdas e interfaces funcionais
Function<Integer, Integer> doubleIt = n -> n * 2;
Predicate<Payment> approved = p -> p.status() == PaymentStatus.APPROVED;
Supplier<String> id = () -> UUID.randomUUID().toString();
Consumer<String> log = System.out::println;          // method reference

// streams
BigDecimal total = items.stream()
    .filter(i -> i.quantity() > 0)
    .map(i -> i.unitPrice().multiply(BigDecimal.valueOf(i.quantity())))
    .reduce(BigDecimal.ZERO, BigDecimal::add);
```

### Classes e constructor

```java
public class Payment {                          // entidade de domínio: sem anotação de framework
    private final String id;
    private final String orderId;
    private final Money amount;
    private PaymentStatus status;

    private Payment(String id, String orderId, Money amount, PaymentStatus status) {   // constructor
        this.id = id;
        this.orderId = orderId;
        this.amount = amount;
        this.status = status;
    }

    public static Payment approved(String orderId, Money amount) {     // factory method
        return new Payment(UUID.randomUUID().toString(), orderId, amount, PaymentStatus.APPROVED);
    }

    public static Payment restore(String id, String orderId, Money amount, PaymentStatus status) {
        return new Payment(id, orderId, amount, status);               // usado pelo repositório
    }

    public String getId() { return id; }        // getters (sem setters → encapsulado)
    public PaymentStatus getStatus() { return status; }
}
```

Modificadores: `public` · `protected` (pacote + subclasses) · *(nenhum)* = package-private · `private` · `static` · `final` · `abstract`.

### Interfaces e herança

```java
public interface PaymentGateway {                           // port
    GatewayResult charge(String orderId, Money amount);
}

public class FakePaymentGateway implements PaymentGateway { // adapter
    private static final BigDecimal LIMIT = new BigDecimal("1000");

    @Override
    public GatewayResult charge(String orderId, Money amount) {
        return amount.amount().compareTo(LIMIT) > 0 ? GatewayResult.rejected("limit exceeded")
                                                    : GatewayResult.approved();
    }
}

public abstract class AggregateRoot {
    private final List<DomainEvent> events = new ArrayList<>();
    protected void addEvent(DomainEvent e) { events.add(e); }
    public List<DomainEvent> pullEvents() { var copy = List.copyOf(events); events.clear(); return copy; }
}

public class Payment extends AggregateRoot {
    public Payment(...) { super(); ... }                    // super() chama o constructor da base
}

// interface pode ter método com corpo
public interface EventPublisher {
    void publish(DomainEvent event);
    default void publishAll(List<DomainEvent> events) { events.forEach(this::publish); }
}

// sealed: restringe quem implementa (bom para resultados)
public sealed interface GatewayResult permits Approved, Rejected {}
```

### Erros (exceptions)

```java
// unchecked (RuntimeException): não obriga try/catch — use para erros de domínio
public class DomainException extends RuntimeException {
    public DomainException(String message) { super(message); }
}
public class PaymentNotFoundException extends DomainException { ... }

// checked (Exception): o compilador obriga tratar ou declarar com "throws"
public void read() throws IOException { ... }

try {
    gateway.charge(orderId, amount);
} catch (DomainException e) {
    ...
} finally {
    ...
}

// try-with-resources: fecha automaticamente
try (var in = Files.newInputStream(path)) { ... }
```

Traduzir para HTTP:

```java
@RestControllerAdvice
public class GlobalExceptionHandler {
    @ExceptionHandler(PaymentNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, String> notFound(PaymentNotFoundException e) {
        return Map.of("error", "PAYMENT_NOT_FOUND", "message", e.getMessage());
    }
}
```

### Concorrência

```java
// Java 21: virtual threads — cada request em uma thread "barata" (spring.threads.virtual.enabled=true)
// Código continua síncrono e simples; bloqueio de I/O não trava thread do SO.

CompletableFuture<Payment> f = CompletableFuture.supplyAsync(() -> process());
f.thenApply(Payment::getId).join();

try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
    executor.submit(() -> doWork());
}
```

### Null-safety

```java
Optional<Payment> p = repository.findByOrderId(orderId);
Payment payment = p.orElseThrow(() -> new PaymentNotFoundException(orderId));
p.ifPresent(x -> ...);
p.map(Payment::getId).orElse("none");

Objects.requireNonNull(orderId, "orderId is required");
```

> Use `Optional` como **retorno**, não como campo ou parâmetro.

### Pacotes / imports

```java
package com.shopflow.payment.domain.entities;   // primeira linha; espelha a pasta

import java.math.BigDecimal;
import java.util.*;                             // evite wildcard em código real
import static org.assertj.core.api.Assertions.assertThat;   // import estático
```

### Annotations do Spring (e afins)

| Annotation | Onde | Para quê |
|---|---|---|
| `@SpringBootApplication` | classe main | Liga auto-config e component scan |
| `@RestController` | classe | Controller REST (retorno vira JSON) |
| `@RequestMapping("/payments")` | classe/método | Prefixo/rota |
| `@GetMapping` `@PostMapping` `@PutMapping` `@PatchMapping` `@DeleteMapping` | método | Rota HTTP |
| `@PathVariable` `@RequestParam` `@RequestBody` `@RequestHeader` | parâmetro | Partes do request |
| `@ResponseStatus(HttpStatus.CREATED)` | método | Status |
| `@Valid` | parâmetro | Dispara Bean Validation |
| `@NotNull` `@NotBlank` `@Positive` `@Email` `@Size` `@Min` | campo de DTO | Regras de validação (jakarta.validation) |
| `@Component` | classe | Bean genérico |
| `@Service` / `@Repository` | classe | Bean (semântico; `@Repository` traduz exceções de banco) |
| `@Configuration` + `@Bean` | classe / método | Registrar beans manualmente (use para os use cases) |
| `@Autowired` | constructor/campo | Injeção — **desnecessário** com um único constructor |
| `@Qualifier("nome")` / `@Primary` | parâmetro / bean | Escolher entre várias implementações |
| `@Value("${prop}")` | campo/parâmetro | Ler propriedade |
| `@ConfigurationProperties(prefix="app")` | classe/record | Propriedades tipadas |
| `@Transactional` | método/classe | Transação de banco (num bean da infra ou config) |
| `@Entity` `@Table` `@Id` `@Column` `@Enumerated` `@OneToMany` | classe/campo JPA | Mapeamento ORM (**só na infra**) |
| `@KafkaListener(topics="stock.reserved", groupId="payment-service")` | método | Consumidor Kafka |
| `@RabbitListener(queues="...")` | método | Consumidor RabbitMQ |
| `@RestControllerAdvice` + `@ExceptionHandler` | classe / método | Tratamento global de erros |
| `@Test` `@BeforeEach` `@DisplayName` `@ParameterizedTest` | método de teste | JUnit 5 |
| `@ExtendWith(MockitoExtension.class)` `@Mock` `@InjectMocks` | teste | Mockito |
| `@SpringBootTest` | classe de teste | Sobe o contexto inteiro |
| `@WebMvcTest(PaymentController.class)` | classe de teste | Só a camada web |
| `@DataJpaTest` | classe de teste | Só JPA |
| `@MockitoBean` | campo de teste | Substitui bean por mock (Boot 3.4+; antes era `@MockBean`) |
| `@Testcontainers` `@Container` `@ServiceConnection` | teste | Containers reais com config automática |

```java
// presentation/controllers/PaymentController.java
@RestController
@RequestMapping("/payments")
public class PaymentController {
    private final GetPaymentUseCase getPayment;

    public PaymentController(GetPaymentUseCase getPayment) {   // injeção por constructor
        this.getPayment = getPayment;
    }

    @GetMapping
    public PaymentOutput byOrder(@RequestParam String orderId) {
        return getPayment.execute(orderId);
    }
}

// presentation/consumers/StockEventsConsumer.java
@Component
public class StockEventsConsumer {
    private final ProcessPaymentUseCase processPayment;
    private final ObjectMapper mapper;

    public StockEventsConsumer(ProcessPaymentUseCase processPayment, ObjectMapper mapper) { ... }

    @KafkaListener(topics = "stock.reserved", groupId = "payment-service")
    public void onStockReserved(String message) throws JsonProcessingException {
        var event = mapper.readValue(message, StockReservedEnvelope.class);
        processPayment.execute(new ProcessPaymentInput(event.payload().orderId(), event.payload().total()));
    }
}
```

### Injeção de dependência

```java
// application/usecases/ProcessPaymentUseCase.java — Java puro, sem Spring
public class ProcessPaymentUseCase {
    private final PaymentRepository payments;
    private final PaymentGateway gateway;
    private final EventPublisher publisher;

    public ProcessPaymentUseCase(PaymentRepository payments, PaymentGateway gateway, EventPublisher publisher) {
        this.payments = payments;
        this.gateway = gateway;
        this.publisher = publisher;
    }

    public PaymentOutput execute(ProcessPaymentInput input) { ... }
}

// infra/config/UseCaseConfig.java — a "cola"
@Configuration
public class UseCaseConfig {
    @Bean
    ProcessPaymentUseCase processPaymentUseCase(PaymentRepository r, PaymentGateway g, EventPublisher p) {
        return new ProcessPaymentUseCase(r, g, p);
    }
}

// adapters viram beans com @Component/@Repository
@Repository
public class JpaPaymentRepository implements PaymentRepository { ... }
```

### Testes (JUnit 5 + AssertJ + Mockito)

```java
class ProcessPaymentUseCaseTest {
    private InMemoryPaymentRepository repo;
    private EventPublisher publisher;
    private ProcessPaymentUseCase sut;

    @BeforeEach
    void setUp() {
        repo = new InMemoryPaymentRepository();
        publisher = mock(EventPublisher.class);
        sut = new ProcessPaymentUseCase(repo, new FakePaymentGateway(), publisher);
    }

    @Test
    void approvesPaymentUpToLimit() {
        var out = sut.execute(new ProcessPaymentInput("ord-1", new BigDecimal("100")));

        assertThat(out.status()).isEqualTo(PaymentStatus.APPROVED);
        verify(publisher).publish(any(PaymentApproved.class));
    }

    @Test
    void rejectsAboveLimit() {
        var out = sut.execute(new ProcessPaymentInput("ord-1", new BigDecimal("1500")));
        assertThat(out.status()).isEqualTo(PaymentStatus.REJECTED);
    }

    @Test
    void negativeMoneyThrows() {
        assertThatThrownBy(() -> new Money(new BigDecimal("-1")))
            .isInstanceOf(DomainException.class);
    }
}

// integração com Postgres real
@SpringBootTest
@Testcontainers
class JpaPaymentRepositoryIT {
    @Container @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17");
    ...
}
```

### Kafka e RabbitMQ

```java
// produtor
@Component
public class KafkaEventPublisher implements EventPublisher {
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    ...
    @Override
    public void publish(DomainEvent event) {
        kafka.send(event.topic(), event.aggregateId(), toJson(event));   // (tópico, chave, valor)
    }
}

// consumidor: @KafkaListener (ver acima)
// RabbitMQ: spring-boot-starter-amqp → RabbitTemplate.convertAndSend(exchange, routingKey, msg) / @RabbitListener
```

---

## 4. Go

### Arquivo principal

```go
// cmd/notification/main.go
package main                                   // pacote main + func main = executável

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/<seu-user>/shopflow/notification-service/internal/config"
	"github.com/<seu-user>/shopflow/notification-service/internal/notification/application"
	"github.com/<seu-user>/shopflow/notification-service/internal/notification/infra/email"
	"github.com/<seu-user>/shopflow/notification-service/internal/notification/infra/messaging"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()

	// contexto cancelado em Ctrl+C / SIGTERM (k8s manda SIGTERM ao parar o pod)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// injeção de dependência manual (composition root)
	sender := email.NewLogSender(logger)
	useCase := application.NewSendNotification(sender)
	consumer, err := messaging.NewRabbitMQConsumer(cfg.RabbitURL, useCase, logger)
	if err != nil {
		logger.Error("connect rabbitmq", "err", err)
		os.Exit(1)
	}
	defer consumer.Close()

	if err := consumer.Run(ctx); err != nil {     // bloqueia até ctx ser cancelado
		logger.Error("consumer stopped", "err", err)
		os.Exit(1)
	}
}
```

### Ferramentas e comandos

| Ação | Comando |
|---|---|
| Criar módulo | `go mod init github.com/<seu-user>/shopflow/notification-service` |
| Adicionar dep | `go get github.com/rabbitmq/amqp091-go` |
| Limpar deps | `go mod tidy` |
| Rodar | `go run ./cmd/notification` |
| Build | `go build -o bin/notification ./cmd/notification` |
| Testes | `go test ./...` · `go test -v -run TestSend ./internal/...` · `go test -race ./...` · `go test -cover ./...` |
| Formatar / checar | `gofmt -w .` (ou `go fmt ./...`) · `go vet ./...` |

### Variáveis e constantes

```go
var attempts int          // zero value: 0 ("" para string, false, nil para ponteiro/slice/map)
var name string = "Ana"
count := 1                // declaração curta com inferência (só dentro de função) — a mais usada

const MaxRetries = 3      // constante (em tempo de compilação)

const (                   // "enum" com iota
	TemplateOrderConfirmed Template = iota   // 0
	TemplateOrderCancelled                    // 1
)
```

> Variável declarada e não usada = **erro de compilação**. Import não usado também.

### Tipos

```go
// básicos
var i int = 1; var i64 int64; var f float64; var s string; var b bool
var by byte            // alias de uint8
var r rune = 'á'       // alias de int32 (um caractere Unicode)

// array (tamanho fixo — raro) e slice (dinâmico — o comum)
arr := [3]int{1, 2, 3}
ids := []string{"a", "b"}
ids = append(ids, "c")
part := ids[0:2]
buf := make([]byte, 0, 1024)   // len 0, cap 1024

// map
stock := map[string]int{"p1": 10}
stock["p2"] = 5
qty, ok := stock["p3"]          // ok == false se não existe
delete(stock, "p1")

// struct
type Notification struct {
	To       string            `json:"to"`        // struct tag: nome no JSON
	Template string            `json:"template"`
	Data     map[string]any    `json:"data"`
}

// ponteiro
n := &Notification{To: "a@b.com"}   // *Notification
n.To = "x@y.com"                    // acesso automático, sem ->

// tipo nomeado sobre outro tipo
type Template string
type Email string

// any = interface{} (qualquer coisa)
// generics (1.18+)
func First[T any](items []T) (T, bool) {
	var zero T
	if len(items) == 0 {
		return zero, false
	}
	return items[0], true
}
```

### Funções

```go
func Add(a, b int) int { return a + b }

// múltiplos retornos — padrão (valor, erro)
func Parse(body []byte) (*Notification, error) { ... }

// retornos nomeados
func Split(sum int) (x, y int) {
	x = sum / 2
	y = sum - x
	return
}

// variádica
func Sum(values ...int) int { ... }

// função como valor / closure
double := func(n int) int { return n * 2 }

// defer: roda ao sair da função (LIFO) — fechar recursos
func read() error {
	f, err := os.Open("x")
	if err != nil {
		return err
	}
	defer f.Close()
	...
}
```

> Nome com **letra maiúscula = exportado** (público fora do pacote). Minúscula = privado ao pacote.

### "Classes": structs, métodos e constructor

Go não tem classes, herança nem `constructor`. Tem **structs + métodos + função `NewX`**.

```go
package domain

var (
	ErrInvalidEmail    = errors.New("invalid email")      // erros sentinela
	ErrUnknownTemplate = errors.New("unknown template")
)

type Notification struct {
	to       string           // minúsculo = encapsulado (só o pacote domain acessa)
	template Template
	data     map[string]any
}

// "constructor" por convenção: valida e retorna (*T, error)
func NewNotification(to string, template Template, data map[string]any) (*Notification, error) {
	if !strings.Contains(to, "@") {
		return nil, ErrInvalidEmail
	}
	if !template.IsValid() {
		return nil, ErrUnknownTemplate
	}
	return &Notification{to: to, template: template, data: data}, nil
}

// método com receiver de VALOR (recebe cópia — use para leitura)
func (n Notification) To() string { return n.to }

// método com receiver de PONTEIRO (pode alterar; evita cópia) — o mais comum
func (n *Notification) MarkSent() { ... }

// método em tipo nomeado
func (t Template) IsValid() bool {
	return t == "order-confirmed" || t == "order-cancelled"
}
```

### Interfaces e composição

```go
// application/ports.go — interface definida por QUEM USA, pequena
type EmailSender interface {
	Send(ctx context.Context, n *domain.Notification) error
}

// infra/email/log_sender.go — implementa SEM declarar "implements"
type LogSender struct{ logger *slog.Logger }

func NewLogSender(l *slog.Logger) *LogSender { return &LogSender{logger: l} }

func (s *LogSender) Send(ctx context.Context, n *domain.Notification) error {
	if strings.HasSuffix(n.To(), "@fail.com") {
		return errors.New("smtp unavailable")
	}
	s.logger.Info("email sent", "to", n.To())
	return nil
}

// garantia em tempo de compilação de que LogSender satisfaz a interface
var _ application.EmailSender = (*LogSender)(nil)

// composição por embedding (no lugar de herança)
type Base struct{ ID string }
func (b Base) Identifier() string { return b.ID }

type Order struct {
	Base              // campos e métodos de Base "sobem" para Order
	Total int64
}
// o := Order{Base: Base{ID: "1"}}; o.Identifier()
```

Máxima de Go: **"aceite interfaces, retorne structs"**.

### Erros

```go
// erro é um valor retornado, não exceção
n, err := domain.NewNotification(to, tpl, data)
if err != nil {
	return fmt.Errorf("build notification: %w", err)   // %w embrulha mantendo a causa
}

// checar o tipo/causa
if errors.Is(err, domain.ErrInvalidEmail) { ... }       // compara com sentinela (atravessa o wrap)

var target *ValidationError
if errors.As(err, &target) { ... }                      // extrai tipo customizado

// erro customizado = qualquer tipo com método Error() string
type ValidationError struct{ Field string }
func (e *ValidationError) Error() string { return "invalid " + e.Field }

// panic/recover: só para bug irrecuperável, nunca para fluxo de negócio
```

### Concorrência

```go
// goroutine: função rodando concorrentemente (muito barata)
go worker(ctx, msgs)

// channel: comunicação entre goroutines
jobs := make(chan Job, 10)        // com buffer 10
jobs <- Job{}                     // envia
job := <-jobs                     // recebe
close(jobs)
for j := range jobs { ... }       // lê até fechar

// select: espera o primeiro canal pronto
for {
	select {
	case <-ctx.Done():             // cancelamento (shutdown)
		return ctx.Err()
	case msg, ok := <-deliveries:
		if !ok {
			return errors.New("channel closed")
		}
		handle(msg)
	}
}

// esperar várias goroutines
var wg sync.WaitGroup
for i := 0; i < 3; i++ {
	wg.Add(1)
	go func() {
		defer wg.Done()
		work()
	}()
}
wg.Wait()

// proteger estado compartilhado
var mu sync.Mutex
mu.Lock(); count++; mu.Unlock()
```

`context.Context` é sempre o **primeiro parâmetro** de funções que fazem I/O: carrega cancelamento, deadline e valores do request.

### nil

```go
var p *Notification     // nil
var s []string          // nil, mas len(s) == 0 e append funciona
var m map[string]int    // nil: ler funciona, ESCREVER dá panic → use make(map[string]int)
var e error             // nil = sem erro

if p == nil { ... }
```

### Pacotes / imports

```go
package application        // nome do pacote = nome da pasta (minúsculo, sem _ nem -)

import (
	"context"                                                         // stdlib
	"github.com/<seu-user>/shopflow/notification-service/internal/notification/domain"   // do módulo
	amqp "github.com/rabbitmq/amqp091-go"                             // com alias
)
```

- `internal/` só pode ser importado por código do mesmo módulo.
- Não existe import circular: se A importa B, B não importa A (isso força a regra de dependência da Clean Arch).

### Decorators/annotations?

Go **não tem**. Equivalentes:

| Necessidade | Em Go |
|---|---|
| Mapear JSON / DB | **struct tags**: `` `json:"to" db:"to_address"` `` |
| Rotas HTTP | `http.HandleFunc("GET /health", handler)` (Go 1.22+ aceita método + path) |
| Middleware | função que recebe e retorna `http.Handler` |
| DI | Manual no `main.go` (constructors `NewX`) |
| Validação | código explícito no `NewX` |

```go
// presentation/http/health.go
mux := http.NewServeMux()
mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
})
go http.ListenAndServe(":8081", mux)
```

### Testes (pacote `testing`)

```go
// internal/notification/domain/notification_test.go
package domain

import (
	"errors"
	"testing"
)

func TestNewNotification_InvalidEmail(t *testing.T) {
	_, err := NewNotification("sem-arroba", "order-confirmed", nil)
	if !errors.Is(err, ErrInvalidEmail) {
		t.Fatalf("expected ErrInvalidEmail, got %v", err)
	}
}

// table-driven test: o padrão em Go
func TestTemplate_IsValid(t *testing.T) {
	tests := []struct {
		name string
		tpl  Template
		want bool
	}{
		{"confirmed", "order-confirmed", true},
		{"cancelled", "order-cancelled", true},
		{"unknown", "promo", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tpl.IsValid(); got != tt.want {
				t.Errorf("IsValid() = %v, want %v", got, tt.want)
			}
		})
	}
}
```

```go
// application/send_notification_test.go — fake escrito à mão (sem framework de mock)
type fakeSender struct {
	sent []*domain.Notification
	err  error
}

func (f *fakeSender) Send(_ context.Context, n *domain.Notification) error {
	f.sent = append(f.sent, n)
	return f.err
}

func TestSendNotification_CallsSender(t *testing.T) {
	sender := &fakeSender{}
	uc := NewSendNotification(sender)

	err := uc.Execute(context.Background(), Input{To: "a@b.com", Template: "order-confirmed"})

	if err != nil {
		t.Fatal(err)
	}
	if len(sender.sent) != 1 {
		t.Fatalf("expected 1 send, got %d", len(sender.sent))
	}
}
```

Teste de integração: use `testing.Short()` para pular (`go test -short ./...`) ou build tag `//go:build integration`.
Opcional: `github.com/stretchr/testify` (`assert`, `require`) para asserts mais curtos.

### RabbitMQ (e Kafka)

```go
import amqp "github.com/rabbitmq/amqp091-go"

conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
ch, err := conn.Channel()

// declarar topologia (idempotente)
ch.ExchangeDeclare("notifications", "direct", true, false, false, false, nil)
ch.ExchangeDeclare("notifications.dlx", "direct", true, false, false, false, nil)
ch.QueueDeclare("notifications.email", true, false, false, false, amqp.Table{
	"x-dead-letter-exchange":    "notifications.dlx",
	"x-dead-letter-routing-key": "email.send",
})
ch.QueueBind("notifications.email", "email.send", "notifications", false, nil)
ch.QueueDeclare("notifications.email.dlq", true, false, false, false, nil)
ch.QueueBind("notifications.email.dlq", "email.send", "notifications.dlx", false, nil)

ch.Qos(10, 0, false)    // prefetch

msgs, err := ch.Consume("notifications.email", "notification-service", false /* autoAck */, false, false, false, nil)

for d := range msgs {
	if err := handle(ctx, d.Body); err != nil {
		retries, _ := d.Headers["x-retry-count"].(int32)
		if retries >= 3 {
			d.Nack(false, false)            // sem requeue → DLQ
			continue
		}
		// republica com retry+1 e confirma a original
		ch.PublishWithContext(ctx, "notifications", "email.send", false, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Headers:      amqp.Table{"x-retry-count": retries + 1},
			Body:         d.Body,
		})
		d.Ack(false)
		continue
	}
	d.Ack(false)
}
```

Kafka (se quiser que o Go consuma eventos também): `github.com/segmentio/kafka-go` — `kafka.NewReader(kafka.ReaderConfig{Brokers, GroupID, Topic})` e `reader.FetchMessage` + `reader.CommitMessages`.
