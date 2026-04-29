# Проект: Budget Analytics (nerpochka)

## Контекст
Проект — ETL + аналитика бюджетных данных из CSV.

Данные лежат в:
data/incoming/
- 1. РЧБ
- 2. Соглашения
- 3. ГЗ
- 4. Выгрузка БУАУ

## Архитектура
- Postgres
- ETL (Python + pandas)
- Backend (FastAPI)
- Docker Compose

## ВАЖНО

Контейнеры используют путь:

DATA_DIR=/data/incoming

В docker-compose:
- ./data монтируется в /data
- ./etl монтируется в /etl

## Проблема

ETL не находит CSV, потому что DATA_DIR должен быть /data/incoming

## Задачи для агента

1. Проверить run_import.py
2. Убедиться, что:
   - используется Path(os.getenv("DATA_DIR"))
   - поиск файлов корректный (suffix.lower() == ".csv")
3. Добавить логирование:
   - путь DATA_DIR
   - список найденных файлов
4. Проверить docker-compose.yml:
   - volumes:
       - ./data:/data
5. Исправить при необходимости