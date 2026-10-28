# Очередь заданий на PostgreSQL

API ставит задания в очередь, воркеры забирают их через `SELECT … FOR UPDATE SKIP LOCKED`, имитируют генерацию и отчитываются. Если воркер пропал посреди задания, через 10 минут без heartbeat задание возвращается в очередь и его берёт другой.

## Запуск

```bash
docker compose up --build
```

Поднимутся `db` (Postgres 16), одноразовый `migrate` (`alembic upgrade head`), `api` на `localhost:8000` и два воркера. Накидать работы:

```bash
for i in $(seq 12); do
  curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
    -d "{\"prompt\": \"фраза $i\", \"params\": {\"voice\": \"aigerim\"}}" > /dev/null
done
docker compose logs -f worker-1 worker-2
```

Воркер «генерирует» 1–4 секунды, примерно каждое десятое задание у него падает с ошибкой провайдера, чтобы было видно `/fail`.

### Проверка: воркер умер посреди задания

Lease можно укоротить, чтобы не ждать 10 минут:

```bash
LEASE_TIMEOUT_SECONDS=15 REAPER_INTERVAL_SECONDS=5 docker compose up -d --build --wait
# поставить 12 заданий, как выше, и сразу:
docker compose kill -s SIGKILL worker-1
```

Что было на моей машине: worker-1 взял задание `6847323f…` («фраза 4») и был убит. Через 15 секунд API вернул задание в очередь, worker-2 его доделал со второй попытки.

```
worker-1  11:08:28 worker claimed 6847323f-1ec7-47e6-aa62-a196921766e4 (attempt 1)
api       job 6847323f-1ec7-47e6-aa62-a196921766e4 lease expired, now queued
worker-2  11:08:47 worker failed 3585f754-…: RuntimeError: simulated provider error
worker-2  11:08:54 worker done 6847323f-1ec7-47e6-aa62-a196921766e4: [aigerim, 2.9 s] фраза 4

 status | worker_id | attempts | count
--------+-----------+----------+-------
 done   | worker-1  |        1 |     1
 done   | worker-2  |        2 |     1     <- то самое задание
 done   | worker-2  |        1 |     9
 failed | worker-2  |        1 |     1
```

## API

| Метод | Кто вызывает | Что делает |
|---|---|---|
| `POST /jobs` | клиент | Создать задание: `{"prompt": "...", "params": {...}, "max_attempts": 3}` → `201`, статус `queued` |
| `GET /jobs/{id}` | клиент | Состояние задания, `404` если нет |
| `POST /jobs/claim` | воркер | Взять самое старое задание из очереди → `200` и задание в статусе `running`, или `204`, если очередь пуста |
| `POST /jobs/{id}/heartbeat` | воркер | Продлить lease на долгой генерации |
| `POST /jobs/{id}/complete` | воркер | `{"result": "текст или ссылка на файл"}` → `done` |
| `POST /jobs/{id}/fail` | воркер | `{"error": "..."}` → `failed` |

Эндпоинты воркера требуют `Authorization: Bearer <token>`. Токены задаются в `WORKER_TOKENS=worker-1:token,worker-2:token`, и `worker_id` берётся из токена, а не из тела запроса: воркер не может представиться чужим именем. `complete`, `fail` и `heartbeat` проходят, только если задание сейчас `running` и принадлежит этому воркеру, иначе `409` с текущим статусом.

`POST /jobs` и `GET /jobs/{id}` открыты: в настоящей системе их закрывает API-ключ клиента на шлюзе, здесь авторизация нужна там, где от неё зависит корректность очереди.

Swagger: `localhost:8000/docs`.

| Переменная | По умолчанию | Что делает |
|---|---|---|
| `LEASE_TIMEOUT_SECONDS` | 600 | Сколько ждать heartbeat, прежде чем считать воркер мёртвым |
| `MAX_RUNTIME_SECONDS` | 3600 | Сколько вообще может выполняться задание, даже если heartbeat приходят |
| `REAPER_INTERVAL_SECONDS` | 30 | Как часто искать зависшие задания |
| `WORKER_TOKENS` | в compose два dev-токена | `worker-id:token` через запятую |

## Статусы

```
queued ──claim──▶ running ──complete──▶ done
   ▲                 │  └────fail─────▶ failed
   └─lease истёк─────┤
     или max runtime │
                     └─то же, но попытки кончились─▶ failed
```

## Схема

Одна таблица, миграция `migrations/versions/0001_create_jobs.py`:

- `id uuid` — наружу уходит uuid, а не автоинкремент, чтобы нельзя было перебрать чужие задания;
- `status text` с `CHECK` на четыре значения и ещё один `CHECK`: у `running` обязательно есть `worker_id` и `heartbeat_at`. Даже ручной `UPDATE` в psql не оставит задание в состоянии «выполняется, но непонятно кем»;
- `attempts`, `max_attempts` — сколько раз задание брали и сколько можно;
- `claimed_at`, `heartbeat_at`, `finished_at`, `created_at`, `updated_at`. `created_at` по умолчанию `clock_timestamp()`, а не `now()`: у заданий, вставленных одной транзакцией, время всё равно разное, и порядок FIFO сохраняется;
- два частичных индекса: по `created_at` для `queued` (их сканирует claim) и по `heartbeat_at` для `running` (их сканирует reaper). Завершённые задания копятся, но ни в один из индексов не попадают.

## Тесты

Одной командой, Postgres поднимается сам:

```bash
docker compose --profile test run --rm --build tests
```

Или с хоста: `docker compose up -d db && uv run pytest`. Тесты создают отдельную базу `jobs_test` и в начале каждого прогона накатывают миграции, откатывают их до нуля и накатывают снова, так что `downgrade` тоже не остаётся непроверенным.

```
tests/test_api.py::test_job_lifecycle PASSED
tests/test_api.py::test_fail_records_the_error PASSED
tests/test_api.py::test_claim_on_empty_queue_is_204 PASSED
tests/test_api.py::test_worker_endpoints_need_a_valid_token[headers0] PASSED
tests/test_api.py::test_worker_endpoints_need_a_valid_token[headers1] PASSED
tests/test_api.py::test_worker_endpoints_need_a_valid_token[headers2] PASSED
tests/test_api.py::test_worker_endpoints_need_a_valid_token[headers3] PASSED
tests/test_api.py::test_worker_id_comes_from_the_token_not_from_the_request PASSED
tests/test_api.py::test_another_worker_cannot_finish_or_extend_my_job PASSED
tests/test_api.py::test_finished_job_cannot_change_again[complete] PASSED
tests/test_api.py::test_finished_job_cannot_change_again[fail] PASSED
tests/test_api.py::test_queued_job_cannot_be_completed_without_claim PASSED
tests/test_api.py::test_unknown_job_is_404 PASSED
tests/test_api.py::test_invalid_job_is_rejected PASSED
tests/test_api.py::test_two_workers_over_http_split_the_queue PASSED
tests/test_claim.py::test_claim_takes_the_oldest_queued_job PASSED
tests/test_claim.py::test_jobs_created_in_one_transaction_keep_their_order PASSED
tests/test_claim.py::test_claim_on_empty_queue_returns_nothing PASSED
tests/test_claim.py::test_locked_job_is_skipped_not_waited_for PASSED
tests/test_claim.py::test_concurrent_workers_never_get_the_same_job PASSED
tests/test_reaper.py::test_stale_job_goes_back_to_the_queue PASSED
tests/test_reaper.py::test_job_inside_the_lease_is_left_alone PASSED
tests/test_reaper.py::test_requeued_job_can_be_taken_by_another_worker_and_the_old_one_loses_it PASSED
tests/test_reaper.py::test_job_fails_after_max_attempts PASSED
tests/test_reaper.py::test_heartbeat_keeps_a_long_job_alive PASSED
tests/test_reaper.py::test_finished_jobs_are_never_requeued PASSED
tests/test_reaper.py::test_alive_but_hanging_job_is_requeued_after_max_runtime PASSED
tests/test_reaper.py::test_reaper_loop_runs_in_the_background PASSED
============================== 28 passed in 1.34s ==============================
```

Про гонки два теста. `test_locked_job_is_skipped_not_waited_for` держит открытую транзакцию с захваченной строкой и проверяет, что второй claim за 2 секунды получает следующее задание, а не ждёт блокировку. Без `SKIP LOCKED` он упадёт по таймауту. `test_concurrent_workers_never_get_the_same_job` гоняет 10 параллельных воркеров по 300 заданиям на отдельных соединениях и проверяет, что каждое задание выдано ровно один раз.

## Решения и компромиссы

**Claim — один `UPDATE`.** `UPDATE jobs SET status='running', … WHERE id = (SELECT id … FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *`. Выбор и захват происходят в одном запросе и одной транзакции, промежутка, в который мог бы влезть второй воркер, нет. `SKIP LOCKED` делает так, что строку, которую прямо сейчас захватывает другой воркер, мы просто не видим, и десять воркеров не выстраиваются в очередь за одной строкой.

**Lease считается от последнего heartbeat, а не от claim.** ТЗ говорит «висят в running больше 10 минут». Если считать от `claimed_at`, длинная генерация (видео, длинное аудио) будет отбираться у живого воркера, который просто долго работает. Поэтому воркер раз в минуту шлёт heartbeat, а reaper смотрит на `heartbeat_at`. Для коротких заданий разницы нет: heartbeat при claim равен времени claim.

Но живой воркер тоже может зависнуть: запрос к провайдеру не отвечает, а heartbeat идёт из соседней корутины. На этот случай есть второй порог, `MAX_RUNTIME_SECONDS` от момента claim. Reaper возвращает такое задание в очередь, следующий heartbeat воркера получает `409`, и воркер бросает генерацию.

**Опоздавший воркер не перезапишет результат.** `complete`, `fail` и `heartbeat` — это `UPDATE … WHERE status='running' AND worker_id = :me`. Если задание уже вернули в очередь и отдали другому, старый воркер получит `409` и выбросит результат. Heartbeat получает тот же `409`, так что воркер узнаёт о потере задания ещё во время генерации и может её прервать.

**Попытки ограничены.** Задание, на котором воркер стабильно умирает (OOM на огромном входе), иначе крутилось бы по кругу вечно. После `max_attempts` reaper переводит его в `failed` с понятной ошибкой.

**Reaper живёт в процессе API.** Это фоновая задача, которая раз в 30 секунд выполняет один `UPDATE` с тем же `SKIP LOCKED`. Если API запущено в нескольких экземплярах, они не мешают друг другу и не возвращают одно задание дважды. Отдельный сервис или cron был бы ещё одной вещью, за которой надо следить.

**Транзакция коммитится до ответа.** Сессия открывается в теле обработчика, а не в yield-зависимости FastAPI. Иначе коммит claim мог бы случиться уже после того, как воркер получил задание, и его быстрый `/complete` увидел бы ещё `queued`.

**Воркеры ходят в API, а не в базу.** Так у воркеров нет доступа к Postgres, их можно запускать где угодно (GPU-машина у провайдера), а вся логика переходов лежит в одном месте, `app/queue.py`.

**`text` + `CHECK`, а не enum Postgres.** Новый статус (`cancelled`, `scheduled`) добавляется одной миграцией с заменой constraint. `ALTER TYPE … ADD VALUE` нельзя откатить, а удалить значение из enum Postgres не умеет вовсе.

**Воркер переживает перезапуск API.** Результат и ошибка отправляются с повторами и паузой до 30 секунд. Если API так и не ответил, воркер не падает, а пишет в лог и берёт следующее задание: незакрытое задание вернётся в очередь по lease. Сообщение об ошибке собирается как `Тип: текст`, потому что у `TimeoutError()` текст пустой, а пустую ошибку API не примет. По SIGTERM воркер доделывает текущее задание и выходит, на это в compose дано 30 секунд.

**Гарантия — at-least-once.** Если воркер доделал генерацию, но умер до `/complete`, задание выполнится ещё раз. Для генерации это нормально, но запись результата лучше делать идемпотентной (ключ файла = id задания).

**Опрос, а не LISTEN/NOTIFY.** Пустая очередь опрашивается раз в секунду. Для генерации, которая сама идёт секунды, задержка незаметна, а схема проще. Если нужна реакция в миллисекундах, `NOTIFY` в `POST /jobs` и `LISTEN` в воркере добавляются без изменения схемы.

## Что дальше

- `run_after` и экспоненциальная задержка между попытками, чтобы при падении провайдера не долбить его сразу;
- `kind` или очередь у задания, чтобы GPU-воркеры брали только свои задания;
- отмена задания клиентом (`cancelled`), heartbeat тогда вернёт `409`, и воркер остановится;
- архив завершённых заданий: партиционирование по `created_at` или перенос старше N дней;
- метрики: глубина очереди, возраст самого старого `queued`, число возвратов по lease.
