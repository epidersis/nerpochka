<img width="1344" height="768" alt="17774334747ecd" src="https://github.com/user-attachments/assets/724995b6-d110-4b4e-a91f-59e8e4039b14" />

# nerpochka

Nerpochka - система бюджетной аналитики для загрузки CSV-выгрузок, нормализации данных в PostgreSQL, построения витрины показателей и просмотра результата через API/UI с Excel-выгрузкой.

> [!NOTE]
> Материалы проекта (презентация, Demo_UI) [Яндекс Диск](https://disk.yandex.ru/d/UCPNEXK5lYpJXw)

Проект поднимается одной Docker-командой: база данных, backend, ETL и frontend запускаются вместе, а ETL автоматически загружает исходные CSV и пересобирает staging/mart.

## Ссылка на работающий прототип
http://62.109.20.155:5173/

## Возможности

- Импорт CSV из `data/incoming` в raw-слой PostgreSQL.
- Устойчивое чтение CSV с разными разделителями: `,` и `;`.
- Staging-слой для РЧБ, Госзаказа, соглашений и БУ/АУ.
- Витрина `mart.indicators` для аналитики по разделам, КЦСР, объектам и типам показателей.
- REST API для сводки, справочников и XLSX-выгрузки.
- Vue UI с фильтрами, таблицей и интерактивными графиками ECharts.
- Docker Compose запуск всей системы одной командой.

## Стек

- PostgreSQL 16
- Python 3.12
- FastAPI
- SQLAlchemy
- openpyxl
- Vue 3 + Vite
- axios
- ECharts + vue-echarts
- Docker Compose

## Структура проекта

```text
nerpochka/
  demo_ui/
  backend/              FastAPI API и XLSX-экспорт
  data/incoming/        входные CSV-файлы для импорта
  db/init/              SQL-инициализация схем raw/stg/mart
  etl/                  импорт raw, загрузчики staging, сборка mart
  frontend/             Vue UI
  docker-compose.yml    единый запуск сервисов
  .env                  переменные окружения для контейнеров
```

## UI демонстрации

> [!CAUTION]
> В демонстрации используется UI из [demo_ui/](demo_ui)
> 
> В сборке используется другой фронт [frontend/](frontend)

## Быстрый старт

Требования:

- Docker
- Docker Compose

Запуск всего проекта:

```bash
docker compose up --build
```

Запуск в фоне:

```bash
docker compose up --build -d
```

После старта будут доступны:

- Frontend: <http://localhost:5173>
- Backend API: <http://localhost:8000>
- Healthcheck: <http://localhost:8000/health>
- XLSX-выгрузка: <http://localhost:8000/api/analytics/export>
- PostgreSQL: `localhost:5432`

При запуске сервис `etl` автоматически выполняет:

```bash
python run_import.py
python run_pipeline.py
```

После успешного выполнения ETL-контейнер остается живым, чтобы его можно было инспектировать.

## Переменные окружения

Основные переменные лежат в `.env`:

```env
DATA_DIR=/data/incoming
DATABASE_URL=postgresql://budget_user:budget_pass@postgres:5432/budget_analytics
```

Дополнительно ETL и экспорт используют `REPORT_YEAR`. Если переменная не задана, используется `2025`.

Пример:

```env
REPORT_YEAR=2025
```

## Данные

Исходные файлы кладутся в `data/incoming`.

Ожидаемые папки:

```text
data/incoming/
  1. РЧБ/
  2. Соглашения/
  3. ГЗ/
  4. БУАУ/
```

Raw-импорт сохраняет файлы в:

- `raw.import_files`
- `raw.csv_rows`

Повторный импорт безопасен: уже загруженные файлы пропускаются по hash.

## ETL

ETL состоит из двух этапов.

1. Raw-импорт CSV:

```bash
docker compose exec etl python run_import.py
```

2. Пересборка staging и mart:

```bash
docker compose exec etl python run_pipeline.py
```

Единый pipeline последовательно запускает:

- `load_rchb`
- `load_gz`
- `load_agreements`
- `load_buau`
- `build_mart`

Повторно пересобрать данные без перезапуска проекта:

```bash
docker compose exec etl python run_import.py
docker compose exec etl python run_pipeline.py
```

Логи ETL:

```bash
docker compose logs -f etl
```

## Слои данных

### raw

Сырой слой хранит исходные CSV без потери структуры:

- `raw.import_files` - метаданные файлов.
- `raw.csv_rows` - строки CSV в JSONB.

### stg

Очищенный staging-слой:

- `stg.budget_operations` - РЧБ.
- `stg.gz_budget_lines` - бюджетные строки ГЗ.
- `stg.gz_contracts` - контракты и договоры ГЗ.
- `stg.gz_payments` - платежки ГЗ.
- `stg.agreements` - соглашения.
- `stg.buau_operations` - операции БУ/АУ.

Правила staging:

- бюджетные коды хранятся как `text`;
- суммы приводятся к `numeric`;
- даты приводятся к `date`;
- пустые строки заменяются на `NULL`;
- загрузчики не падают при отсутствующих колонках;
- в логах печатается количество обработанных строк.

### mart

Витрина:

- `mart.indicators`

Основные типы показателей:

- `limit`
- `budget_obligation`
- `cash_execution`
- `contract_amount`
- `contract_payment`
- `budget_amount`

Раздел определяется по бюджетным кодам:

- `КИК`: КЦСР содержит `975`;
- `СКК`: КЦСР содержит `970`;
- `Раздел 2/3`: КЦСР содержит `6105`;
- `ОКВ`: заполнен `kdr_code` и он не равен `0`;
- иначе `Другое`.

## API

Базовый URL:

```text
http://localhost:8000
```

### `GET /health`

Проверка доступности backend.

### `GET /api/analytics/summary`

Возвращает сгруппированные суммы из `mart.indicators`.

Фильтры:

- `section`
- `kcsr_code`
- `indicator_type`
- `period_to`

Пример:

```bash
curl "http://localhost:8000/api/analytics/summary?section=КИК"
```

Формат ответа:

```json
[
  {
    "section": "КИК",
    "kcsr_code": "0320497501",
    "object_name": "Наименование объекта",
    "indicator_type": "limit",
    "amount": 1000000
  }
]
```

### `GET /api/analytics/sections`

Список разделов.

### `GET /api/analytics/objects`

Список объектов.

Фильтры:

- `section`
- `q`

### `GET /api/analytics/indicators`

Список типов показателей.

### `GET /api/analytics/export`

XLSX-выгрузка аналитики.

Фильтры:

- `section`
- `kcsr_code`

Пример:

```bash
curl -L "http://localhost:8000/api/analytics/export" -o nerpochka_analytics_export.xlsx
```

## Frontend

Frontend доступен по адресу:

```text
http://localhost:5173
```

В UI есть:

- фильтры по разделу, КЦСР и типу показателя;
- интерактивный график;
- переключатель графика `bar / line / pie`;
- таблица сводных данных;
- кнопка XLSX-выгрузки.

Vite настроен для внешнего доступа:

```js
server: {
  host: true,
  allowedHosts: 'all'
}
```

## Проверочные SQL-запросы

Открыть `psql`:

```bash
docker compose exec postgres psql -U budget_user -d budget_analytics
```

Проверить количество строк:

```sql
select count(*) from raw.csv_rows;
select count(*) from stg.budget_operations;
select count(*) from stg.gz_budget_lines;
select count(*) from stg.gz_contracts;
select count(*) from stg.gz_payments;
select count(*) from mart.indicators;
```

Проверить распределение витрины:

```sql
select section, indicator_type, count(*), sum(amount)
from mart.indicators
group by section, indicator_type
order by section, indicator_type;
```

Проверить, что mart собран за отчетный год:

```sql
select source_type, extract(year from period_to) as year, count(*), sum(amount)
from mart.indicators
group by source_type, extract(year from period_to)
order by source_type, year;
```

## Полезные команды

Посмотреть статус контейнеров:

```bash
docker compose ps
```

Посмотреть логи всех сервисов:

```bash
docker compose logs -f
```

Посмотреть логи backend:

```bash
docker compose logs -f backend
```

Перезапустить backend:

```bash
docker compose restart backend
```

Полностью остановить проект:

```bash
docker compose down
```

Остановить проект и удалить volume PostgreSQL:

```bash
docker compose down -v
```

После удаления volume база будет создана заново из `db/init/001_init.sql`, а данные нужно будет импортировать повторно.

## Разработка

Backend запускается в контейнере с `uvicorn --reload`, поэтому изменения в `backend/app` подхватываются автоматически.

Frontend запускается через Vite dev server:

```bash
docker compose exec frontend npm run dev -- --host
```

Проверка Python-синтаксиса:

```bash
docker compose exec backend python -m py_compile app/services/excel_export.py
docker compose exec etl python -m py_compile run_import.py run_pipeline.py loaders/load_rchb.py transformers/build_mart.py
```

Сборка frontend:

```bash
docker compose exec frontend npm run build
```

## Типовые проблемы

### Frontend не открывается через ngrok или внешний host

Проверьте `frontend/vite.config.js`. Должно быть:

```js
server: {
  host: true,
  allowedHosts: 'all'
}
```

После изменения конфигурации перезапустите frontend:

```bash
docker compose restart frontend
```

### UI пишет "Не удалось загрузить данные"

Проверьте backend:

```bash
curl http://localhost:8000/health
docker compose logs --tail=100 backend
```

Проверьте, что mart заполнен:

```bash
docker compose exec postgres psql -U budget_user -d budget_analytics -c "select count(*) from mart.indicators;"
```

Если mart пустой, пересоберите ETL:

```bash
docker compose exec etl python run_import.py
docker compose exec etl python run_pipeline.py
```

### Изменились CSV, но данные не обновились

Raw-слой пропускает файлы по hash. Если файл уже был импортирован и изменился, проверьте его hash/имя или пересоздайте базу:

```bash
docker compose down -v
docker compose up --build
```

### PostgreSQL не стартует с новой схемой

SQL из `db/init` выполняется только при первом создании volume. Для применения изменений схемы на чистой базе удалите volume:

```bash
docker compose down -v
docker compose up --build
```

## Примечания по методологии

- РЧБ и соглашения являются срезами, поэтому для отчета используется последний срез внутри `REPORT_YEAR`, а не сумма всех месяцев.
- РЧБ за годы, отличные от `REPORT_YEAR`, не загружается в `stg.budget_operations`.
- ГЗ-контракты и платежи в mart фильтруются по `REPORT_YEAR`.
- В XLSX-выгрузке областной и местные бюджеты разносятся по разным блокам колонок.

## Лицензия

Лицензия не указана. Перед использованием вне проекта уточните условия у владельца репозитория.
