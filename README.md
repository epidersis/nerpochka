# nerpochka

## ETL pipeline

Raw CSV files are loaded first with:

```bash
docker compose exec etl python run_import.py
```

Then staging and mart are rebuilt with:

```bash
docker compose exec etl python run_pipeline.py
```

## Проверочные SQL-запросы

После выполнения pipeline должны возвращать данные:

```sql
select count(*) from stg.budget_operations;
select count(*) from stg.gz_budget_lines;
select count(*) from stg.gz_contracts;
select count(*) from stg.gz_payments;
select count(*) from mart.indicators;

select section, indicator_type, count(*), sum(amount)
from mart.indicators
group by section, indicator_type
order by section, indicator_type;
```
