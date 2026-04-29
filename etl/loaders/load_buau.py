from __future__ import annotations

from typing import Any

from sqlalchemy import text

from etl_utils import (
    bulk_insert,
    clean_code,
    clean_date,
    clean_decimal,
    clean_text,
    engine,
    fetch_raw_files,
    fetch_raw_rows,
    first_value,
    truncate_table,
)


def ensure_table() -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            create table if not exists stg.buau_operations (
                id bigserial primary key,
                import_file_id bigint references raw.import_files(id),
                operation_date date,
                budget_name text,
                organization_name text,
                provider_name text,
                amount numeric(18,2),
                amount_with_refund numeric(18,2),
                refund_amount numeric(18,2),
                kfsr_code text,
                kcsr_code text,
                kvr_code text,
                kesr_code text,
                kdr_code text,
                kde_code text,
                kdf_code text
            )
        """))


def _operation(data: dict[str, Any], import_file_id: int) -> dict[str, Any]:
    return {
        "import_file_id": import_file_id,
        "operation_date": clean_date(first_value(data, ("Дата проводки", "operation_date"))),
        "budget_name": clean_text(first_value(data, ("Бюджет", "budget_name"))),
        "organization_name": clean_text(first_value(data, ("Организация", "organization_name"))),
        "provider_name": clean_text(first_value(data, ("Орган, предоставляющий субсидии", "provider_name"))),
        "amount": clean_decimal(first_value(data, ("Выплаты - Исполнение", "amount"))),
        "amount_with_refund": clean_decimal(first_value(data, ("Выплаты с учетом возврата",))),
        "refund_amount": clean_decimal(first_value(data, ("Выплаты - Восстановление выплат - год",))),
        "kfsr_code": clean_code(first_value(data, ("КФСР", "kfsr_code"))),
        "kcsr_code": clean_code(first_value(data, ("КЦСР", "kcsr_code"))),
        "kvr_code": clean_code(first_value(data, ("КВР", "kvr_code"))),
        "kesr_code": clean_code(first_value(data, ("КОСГУ", "kesr_code"))),
        "kdr_code": clean_code(first_value(data, ("Код субсидии", "kdr_code"))),
        "kde_code": clean_code(first_value(data, ("КВФО", "kde_code"))),
        "kdf_code": clean_code(first_value(data, ("Отраслевой код", "kdf_code"))),
    }


def run() -> int:
    print("load_buau: ensure and truncate stg.buau_operations")
    ensure_table()
    truncate_table("stg.buau_operations")

    total = 0
    for file in fetch_raw_files("4. Выгрузка БУАУ"):
        rows = []
        for raw_row in fetch_raw_rows(file["id"]):
            parsed = _operation(dict(raw_row["data"]), file["id"])
            if parsed["budget_name"] == "Итого":
                continue
            if parsed["kcsr_code"] or parsed["amount"] is not None:
                rows.append(parsed)

        inserted = bulk_insert("stg.buau_operations", rows)
        total += inserted
        print(f"load_buau: {file['file_name']}: inserted {inserted}")

    print(f"load_buau: done, inserted {total}")
    return total


if __name__ == "__main__":
    run()
