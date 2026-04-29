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
    parse_period,
    truncate_table,
)


def ensure_table() -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            create table if not exists stg.agreements (
                id bigserial primary key,
                import_file_id bigint references raw.import_files(id),
                period_from date,
                period_to date,
                documentclass_id text,
                budget_id text,
                document_id text,
                amount numeric(18,2),
                agreement_number text,
                estimate_name text,
                recipient_name text,
                kadmr_code text,
                kfsr_code text,
                kcsr_code text,
                kvr_code text,
                kesr_code text,
                kdr_code text,
                kde_code text,
                kdf_code text
            )
        """))


def _agreement(data: dict[str, Any], import_file_id: int) -> dict[str, Any]:
    period_from, period_to = parse_period(first_value(data, ("period_of_date",)))
    return {
        "import_file_id": import_file_id,
        "period_from": period_from,
        "period_to": period_to,
        "documentclass_id": clean_code(first_value(data, ("documentclass_id",))),
        "budget_id": clean_code(first_value(data, ("budget_id",))),
        "document_id": clean_code(first_value(data, ("document_id",))),
        "amount": clean_decimal(first_value(data, ("amount", "amount_1year"))),
        "agreement_number": clean_text(first_value(data, ("reg_number", "main_reg_number"))),
        "estimate_name": clean_text(first_value(data, ("dd_estimate_caption", "caption"))),
        "recipient_name": clean_text(first_value(data, ("dd_recipient_caption", "recipient", "получатель"))),
        "kadmr_code": clean_code(first_value(data, ("kadmr_code", "КАДМ"))),
        "kfsr_code": clean_code(first_value(data, ("kfsr_code", "КФСР"))),
        "kcsr_code": clean_code(first_value(data, ("kcsr_code", "КЦСР"))),
        "kvr_code": clean_code(first_value(data, ("kvr_code", "КВР"))),
        "kesr_code": clean_code(first_value(data, ("kesr_code", "КОСГУ"))),
        "kdr_code": clean_code(first_value(data, ("kdr_code",))),
        "kde_code": clean_code(first_value(data, ("kde_code",))),
        "kdf_code": clean_code(first_value(data, ("kdf_code",))),
    }


def run() -> int:
    print("load_agreements: ensure and truncate stg.agreements")
    ensure_table()
    truncate_table("stg.agreements")

    total = 0
    for file in fetch_raw_files("2. Соглашения"):
        rows = []
        for raw_row in fetch_raw_rows(file["id"]):
            parsed = _agreement(dict(raw_row["data"]), file["id"])
            if parsed["document_id"] or parsed["kcsr_code"] or parsed["amount"] is not None:
                rows.append(parsed)

        inserted = bulk_insert("stg.agreements", rows)
        total += inserted
        print(f"load_agreements: {file['file_name']}: inserted {inserted}")

    print(f"load_agreements: done, inserted {total}")
    return total


if __name__ == "__main__":
    run()
