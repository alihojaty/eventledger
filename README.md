# Reliable Deposit Event Processing System 🚀

A production-grade, fault-tolerant financial deposit processing monorepo built with **Go**, backed by **PostgreSQL** (ACID & Concurrency Control) and **NATS JetStream** (Persistent Messaging & Consumer Grouping)[cite: 1, 2].

---

## 🏛️ Architecture & Core Guarantees

This system is engineered around a strict golden rule for financial systems[cite: 1, 2]:
> **"No valid deposit event shall ever be lost, and no valid event shall ever produce a financial effect more than once."**[cite: 1, 2]

1. **Multi-Module Monorepo & Separation of Concerns:**
    - `pkg/domain`: Pure business rules, arbitrary-precision financial calculations (`shopspring/decimal`), and domain errors (`ErrDuplicateEvent`, `InvalidMessageError`)[cite: 1, 2].
    - `pkg/broker`: NATS JetStream integration (`FileStorage`), deduplication windows, and Dead Letter Queue management[cite: 1, 2].
    - `pkg/dbx`: Generic PostgreSQL connection pooling (`pgx`), retry-ping backoff, and automatic database migrations (`golang-migrate`)[cite: 1, 2].
    - `pkg/config` & `pkg/logger`: Shared environment configuration loader and structured JSON logging (`zap`)[cite: 2].
    - `services/depositor`: Consumer microservice handling PostgreSQL ACID transactions, row-level locking, NATS Consumer Grouping (`PullMaxMessages(1)`), non-blocking retry backoff (`NakWithDelay`), and real-time web observability[cite: 1, 2].
    - `services/publisher`: Standalone event generator microservice simulating valid and invalid deposit traffic over NATS JetStream[cite: 2].

2. **Zero Lost Updates & Concurrency Safety:**
   Balance updates use an atomic upsert with PostgreSQL Row-Level Exclusive Locking[cite: 1, 2]:
   ```sql
   INSERT INTO balances ... ON CONFLICT (user_id, currency) 
   DO UPDATE SET balance = balances.balance + EXCLUDED.balance;
   ```
   Even under a storm of 1,000+ concurrent goroutines, race conditions and lost updates are mathematically impossible[cite: 1, 2].

3. **Poison Pill Isolation (DLQ):**
   Malformed JSON, negative amounts, or unsupported currencies (e.g., `DOGE`) are safely intercepted and published to the `DEPOSITS_DLQ` stream *before* acknowledging the original message, preventing queue blockage while ensuring zero data loss[cite: 1, 2].

4. **Graceful Shutdown (Ordered Shutdown):**
   Upon receiving `SIGINT` or `SIGTERM`, the services stop consuming/publishing new messages (`Drain()`), wait up to `SHUTDOWN_TIMEOUT` for in-flight DB transactions to commit, safely close broker connections, and cleanly close database connections[cite: 1, 2].

---

## 🛠️ Tech Stack
- **Language:** Go 1.26+ (Multi-Module Workspace via `go.work`)[cite: 2]
- **Database:** PostgreSQL 18 (with `pgx/v5` driver)[cite: 1, 2]
- **Message Broker:** NATS JetStream (File Storage)[cite: 1, 2]
- **Financial Math:** `shopspring/decimal` (Zero floating-point rounding errors)[cite: 1, 2]
- **Logging:** `go.uber.org/zap` (Structured JSON logs)[cite: 1, 2]
- **Observability:** Custom Lock-Free Atomic Metrics & Embedded Live Web Dashboard (`:8080`)[cite: 1, 2]

---

## 🚀 Quick Start (For Developers)

### Prerequisites
- Docker & Docker Compose installed on your machine[cite: 1, 2].
- Go 1.27.3+ and `make` (if running locally without Docker)[cite: 1, 2].

### Option A: Run Full Stack via Makefile & Docker Compose (Recommended)
1. Clone the repository:
   ```bash
   git clone [https://github.com/alihojaty/reliable-deposit-processor.git](https://github.com/alihojaty/reliable-deposit-processor.git)
   cd reliable-deposit-processor
   ```
2. Spin up the entire stack (PostgreSQL, NATS, Depositor, and Publisher)[cite: 2]:
   ```bash
   make up
   ```
3. Scale the `depositor` service horizontally to 2 instances[cite: 2]:
   ```bash
   make scale
   ```
4. Open the **Live Control Dashboard** in your browser[cite: 1, 2]:
   👉 **`http://localhost:8080`**[cite: 1, 2]
5. Stop the stack when finished[cite: 2]:
   ```bash
   make down
   ```

### Option B: Run Locally for Development
1. Copy the example environment file[cite: 1, 2]:
   ```bash
   cp .env.example .env
   ```
2. Start infrastructure services only (PostgreSQL & NATS)[cite: 1, 2]:
   ```bash
   docker compose -f deploy/docker-compose.yml up -d postgres nats
   ```
3. Build all modules and binaries[cite: 2]:
   ```bash
   make build
   ```
4. Run `depositor` and `publisher` in separate terminals[cite: 2]:
   ```bash
   go run ./services/depositor/cmd/depositor
   ```
   ```bash
   PUBLISH_ENABLED=true go run ./services/publisher/cmd/publisher
   ```

---

## 🧪 Running Tests, Linting & Chaos Engineering

All workspace modules (`./pkg`, `./services/depositor`, `./services/publisher`) are orchestrated via the root `Makefile`[cite: 2]:

- **Run all unit, concurrency (1,000 concurrent deposits), and chaos recovery tests across all modules:**[cite: 1, 2]
  ```bash
  make test
  ```
- **Run `go vet` and `golangci-lint` across all modules:**[cite: 2]
  ```bash
  make lint
  ```
- **Tidy all module `go.mod` files and sync `go.work`:**[cite: 2]
  ```bash
  make tidy
  ```

---

## 🎛️ Live Web Dashboard Features
By navigating to `http://localhost:8080`, you get[cite: 1, 2]:
- **Real-Time TPS & Metrics Cards:** Live counts for received, processed, duplicate, invalid, DLQ, and retried messages[cite: 1, 2].
- **Worker Pool Controller:** Visually inspect worker statuses and click **"Drain"** to gracefully stop individual workers[cite: 1, 2].
- **Live Event & Chaos Injector:** Test valid deposits, duplicates, invalid currencies, or burst traffic with a single click[cite: 1, 2].
- **PostgreSQL Live Wallet Balances:** Real-time table showing user balances across `USDT`, `BTC`, and `IRT`[cite: 1, 2].

---

## 🧩 Monorepo & Multi-Module Architecture

This repository is structured as a Go **Multi-Module Monorepo** orchestrated via `go.work`[cite: 2]:

```text
eventledger/
├── go.work
├── Makefile
├── pkg/                      # Shared independent module
│   ├── go.mod
│   ├── broker/               # NATS JetStream client & streams
│   ├── config/               # Environment configuration loader
│   ├── dbx/                  # PostgreSQL connection pool & migrations
│   ├── domain/               # DepositEvent model, decimal validation & errors
│   └── logger/               # Structured Zap logger
├── services/
│   ├── depositor/            # Depositor service module (NATS -> PostgreSQL)
│   │   ├── go.mod
│   │   ├── cmd/depositor/main.go
│   │   ├── internal/{repository,worker,metrics,integration}/
│   │   ├── migrations/
│   │   └── Dockerfile
│   └── publisher/            # Publisher service module (Event Simulator)
│       ├── go.mod
│       ├── cmd/publisher/main.go
│       ├── internal/generator/
│       └── Dockerfile
└── deploy/
    └── docker-compose.yml
```

### Package Placement Decisions (`pkg/` vs `internal/`)

| Package Path | Module | Reason for Placement |
| :--- | :--- | :--- |
| `pkg/config` | `pkg` | Shared environment configuration loader (`env` + `godotenv`) needed by both `depositor` and `publisher`[cite: 2]. |
| `pkg/logger` | `pkg` | Standardized structured JSON logger (`zap`) used across all services for uniform observability[cite: 2]. |
| `pkg/domain` | `pkg` | Contains the shared `DepositEvent` and `DLQMessage` contracts, currency constants, and `shopspring/decimal` validation rules shared between producer (`publisher`) and consumer (`depositor`)[cite: 2]. |
| `pkg/broker` | `pkg` | Shared NATS JetStream client, stream provisioning (`DEPOSITS`, `DEPOSITS_DLQ`), and publish/consume helpers used by both services[cite: 2]. |
| `pkg/dbx` | `pkg` | Generic PostgreSQL connection pooling (`pgx`), retry-ping backoff, and migration runner reusable by any database-backed service[cite: 2]. |
| `services/depositor/internal/repository` | `depositor` | Contains SQL transactions and queries specific to the `deposits` and `balances` tables; no other service should access `depositor`'s database schema directly[cite: 2]. |
| `services/depositor/internal/worker` | `depositor` | Consumer group management (`PullMaxMessages(1)`), `NakWithDelay` retry logic, and terminal draining specific to the deposit consumer pipeline[cite: 2]. |
| `services/depositor/internal/metrics` | `depositor` | Lock-free atomic metrics and embedded live web dashboard specific to monitoring the `depositor` service[cite: 2]. |
| `services/depositor/internal/integration` | `depositor` | End-to-end chaos engineering and million-scale streaming pipeline tests verifying `depositor`'s resilience[cite: 2]. |
| `services/publisher/internal/generator` | `publisher` | Event simulation and random deposit generation logic exclusive to the `publisher` simulator service[cite: 2]. |

---

### Architectural Q&A

#### ۱. آیا `go.work` را در گیت کامیت می‌کنیم یا نه؟ چرا؟
**بله، در این پروژه (معماری Monorepo برای سرویس‌ها/اپلیکیشن‌ها) فایل `go.work` را کامیت می‌کنیم.**[cite: 2]
* **دلیل:** طبق مستندات رسمی Go، در پروژه‌های کتابخانه‌ای تک‌ماژوله (Library) معمولاً `go.work` کامیت نمی‌شود تا هر توسعه‌دهنده فضای کاری محلی خودش را داشته باشد[cite: 2]. اما در یک **Application Monorepo** که شامل چندین میکروسرویس به هم وابسته (`pkg`، `depositor`، `publisher`) است، کامیت کردن `go.work` و `go.work.sum` تضمین می‌کند که هر توسعه‌دهنده بلافاصله پس از `git clone`، و همچنین محیط‌های **CI/CD** و **Docker Multi-Stage Builds**، بدون نیاز به تنظیمات دستی، دقیقاً از کدهای محلی درون ریپو استفاده کنند و کل workspace یکپارچه کامپایل شود[cite: 2].

#### ۲. اگر فردا یک سرویس سوم (مثلاً `notifier` یا `withdrawer`) بیاید، چه چیزی را تغییر می‌دهیم و چه چیزی را نه؟
* **چیزهایی که تغییر نمی‌کنند (Zero-Touch):**[cite: 2]
    * کدهای داخل `services/depositor` و `services/publisher` و `Dockerfile`های آن‌ها مطلقاً دست‌نخورده باقی می‌مانند[cite: 2].
    * پکیج‌های زیرساختی داخل `pkg/` (`logger`، `dbx`، `config`، `broker`) بدون تغییر توسط سرویس جدید استفاده می‌شوند[cite: 2].
* **چیزهایی که اضافه یا ویرایش می‌شوند:**[cite: 2]
    1. ساخت دایرکتوری `services/notifier` همراه با `go.mod`، `cmd/notifier/main.go`، `internal/` و `Dockerfile` اختصاصی خودش[cite: 2].
    2. اضافه کردن `./services/notifier` به فایل `go.work` (با `go work use ./services/notifier`) و متغیر `MODULES` در `Makefile`[cite: 2].
    3. اضافه کردن تعریف کانتینر سرویس جدید به `deploy/docker-compose.yml`[cite: 2].

#### ۳. چرا `pkg` و `internal` هر دو لازم‌اند؟ یک مثال از هر کدام در پروژهٔ خودت.
* **`pkg/` برای اشتراک‌گذاری کنترل‌شدهٔ زیرساخت و قراردادها (Code Reuse & Contracts):** از تکرار کد (DRY) بین سرویس‌ها جلوگیری می‌کند[cite: 2]. **مثال در پروژه:** پکیج `pkg/domain` (شامل ساختار `DepositEvent` و اعتبارسنجی مبالغ با `shopspring/decimal`)؛ هم سرویس `publisher` برای تولید پیام معتبر و هم سرویس `depositor` برای اعتبارسنجی پیام دریافتی به یک قرارداد واحد نیاز دارند[cite: 2].
* **`internal/` برای حفظ مرزهای سرویس و کپسوله‌سازی (Encapsulation & Service Boundaries):** کامپایلر Go در سطح کامپایل اجازه نمی‌دهد هیچ کدی خارج از درختِ والدِ `internal` آن را ایمپورت کند[cite: 2]. این کار مانع از ایجاد وابستگی پنهان (Tight Coupling) بین سرویس‌ها می‌شود[cite: 2]. **مثال در پروژه:** پکیج `services/depositor/internal/repository` که تراکنش‌های جدول `balances` را اجرا می‌کند؛ سرویس `publisher` تحت هیچ شرایطی نباید بتواند مستقیماً کدهای دیتابیس `depositor` را صدا بزند، و کامپایلر Go این قانون معماری را در زمان کامپایل تضمین می‌کند[cite: 2].

#### ۴. اگر تراکنش در دیتابیس Commit شود اما برنامه دقیقاً قبل از ارسال `Ack` به NATS کرش کند، چه اتفاقی می‌افتد؟ (`DOC-05`)
* از آنجا که JetStream پیام‌های تحویل‌شده‌ای را که در بازهٔ `AckWait` تأیید (`Ack`) نشده‌اند مجدداً ارسال می‌کند (**At-Least-Once Delivery**)، پس از بالا آمدن مجدد سرویس یا توسط Replica دیگر، همان پیام دوباره دریافت می‌شود.
* هنگام اجرای مجدد `ProcessDeposit`، دستور `INSERT INTO deposits ... ON CONFLICT (event_id) DO NOTHING` به دلیل وجود کلید اصلی `event_id` از تراکنش قبلی، مقدار `RowsAffected = 0` برمی‌گرداند.
* کد برنامه خطای `domain.ErrDuplicateEvent` را تشخیص داده، از اجرای کوئری افزایش موجودی (`balances`) صرف‌نظر می‌کند و بلافاصله پیام را `Ack` می‌نماید. در نتیجه **Exactly-Once Financial Effect** تضمین شده و هیچ افزایش موجودی مضاعفی (Double Spend) رخ نمی‌دهد.

#### ۵. اگر دیتابیس PostgreSQL به مدت ۱۰ دقیقه کامل قطع شود، رفتار سیستم چیست؟ (`DOC-06`)
* سرویس `depositor` به هیچ وجه کرش (`panic` یا `os.Exit`) نمی‌کند و پیام‌ها را نیز به اشتباه وارد DLQ نمی‌کند (زیرا خطای زیرساخت با پیام نامعتبر تفاوت دارد).
* به ازای هر خطای اتصال یا تراکنش، Workerها پیام را با `msg.NakWithDelay(cfg.RetryInterval)` به NATS برمی‌گردانند تا بدون ایجاد Busy-Loop یا فشار CPU، پس از بازهٔ تنظیم‌شده مجدداً تلاش شود.
* استریم `DEPOSITS` از نوع `FileStorage` بدون محدودیت حذف زودهنگام و مصرف‌کننده دارای `MaxDeliver: -1` است؛ بنابراین تمام رویدادهای جدید تولیدشده توسط `publisher` در دیسک JetStream محفوظ می‌مانند و به محض بازگشت PostgreSQL، کل Backlog به صورت خودکار و بدون دخالت دستی پردازش و تسویه می‌شود.

#### ۶. چرا استفاده از `float32` / `float64` برای مبالغ مالی ممنوع است؟ (`DOC-07`)
* اعداد ممیز شناور استاندارد IEEE 754 در مبنای ۲ (Binary Floating-Point) ذخیره می‌شوند و قادر به نمایش دقیق بسیاری از کسرهای دهدهی ساده (مانند `0.1` یا `0.2`) نیستند (برای نمونه در ممیز شناور دوگانه `0.1 + 0.2 = 0.30000000000000004` است).
* در مقیاس هزاران تراکنش مالی هم‌زمان، این خطاهای گرد کردن دودویی انباشته شده و باعث عدم تطابق موجودی کیف‌پول با جمع دفتر کل (`SUM(deposits.amount) != balances.balance`) می‌شوند. به همین دلیل در سراسر این سیستم، مبالغ در JSON به صورت رشته (`string`)، در Go با کتابخانهٔ دهدهی با دقت دلخواه (`shopspring/decimal`) و در PostgreSQL با نوع `NUMERIC(36, 18)` پردازش و ذخیره می‌گردند.

#### ۷. نکات تکمیلی دربارهٔ چرخهٔ حیات دو سرویس و مقایسهٔ `COUNT(deposits)` با `LastSequence` (`DOC-13`، `DOC-14`)
* **چرخهٔ حیات مستقل (`A-13`):** پس از تفکیک معماری به دو میکروسرویس مستقل، هر سرویس Graceful Shutdown مختص به خود را دارد؛ `publisher` تولید رویداد جدید را متوقف کرده و ارسال‌های در جریان را تکمیل می‌کند، و `depositor` دریافت پیام جدید را متوقف (`DrainAll`) کرده و منتظر اتمام تراکنش‌های باز دیتابیس تا سقف `SHUTDOWN_TIMEOUT` می‌ماند.
* **شرط برابری با `LastSequence` (`A-01`):** برابری `COUNT(deposits) == stream.LastSequence` زمانی برقرار است که استریم تمیز بوده، تمامی رویدادهای ارسال‌شده معتبر و دارای `event_id` یکتا باشند و Backlog کامل تخلیه شده باشد. در سناریوهایی که پیام نامعتبر (ارسال به `DEPOSITS_DLQ`) یا پیام تکراری خارج از پنجرهٔ Deduplication ارسال شود، `LastSequence` استریم بزرگ‌تر از `COUNT(deposits)` خواهد بود که رفتار صحیح سیستم است.