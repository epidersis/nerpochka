from __future__ import annotations

from typing import Any

from etl_utils import (
    bulk_insert,
    clean_code,
    clean_date,
    clean_decimal,
    clean_text,
    fetch_raw_files,
    fetch_raw_rows,
    first_value,
    truncate_table,
)


def _budget_line(data: dict[str, Any], import_file_id: int) -> dict[str, Any]:
    return {
        "import_file_id": import_file_id,
        "con_document_id": clean_code(first_value(data, ("con_document_id",))),
        "kfsr_code": clean_code(first_value(data, ("kfsr_code", "КФСР"))),
        "kcsr_code": clean_code(first_value(data, ("kcsr_code", "КЦСР"))),
        "kvr_code": clean_code(first_value(data, ("kvr_code", "КВР"))),
        "kesr_code": clean_code(first_value(data, ("kesr_code", "КОСГУ"))),
        "kdr_code": clean_code(first_value(data, ("kdr_code",))),
        "kde_code": clean_code(first_value(data, ("kde_code",))),
        "kdf_code": clean_code(first_value(data, ("kdf_code",))),
        "amount": clean_decimal(first_value(data, ("amount", "line_amount", "sum"))),
    }


def _contract(data: dict[str, Any], import_file_id: int) -> dict[str, Any]:
    return {
        "import_file_id": import_file_id,
        "con_document_id": clean_code(first_value(data, ("con_document_id",))),
        "con_number": clean_text(first_value(data, ("con_number",))),
        "con_date": clean_date(first_value(data, ("con_date",))),
        "customer_name": clean_text(first_value(data, ("customer_name", "key_zakazchik", "zakazchik_key"))),
        "supplier_name": clean_text(first_value(data, ("supplier_name",))),
        "con_amount": clean_decimal(first_value(data, ("con_amount", "amount"))),
    }


def _payment(data: dict[str, Any], import_file_id: int) -> dict[str, Any]:
    return {
        "import_file_id": import_file_id,
        "con_document_id": clean_code(first_value(data, ("con_document_id",))),
        "payment_id": clean_code(first_value(data, ("platezhka_key", "payment_id"))),
        "payment_date": clean_date(first_value(data, ("platezhka_paydate", "payment_date"))),
        "payment_amount": clean_decimal(first_value(data, ("platezhka_amount", "payment_amount"))),
    }


def _load_file(file, table_name: str, row_builder) -> int:
    rows = []
    for raw_row in fetch_raw_rows(file["id"]):
        parsed = row_builder(dict(raw_row["data"]), file["id"])
        if parsed.get("con_document_id"):
            rows.append(parsed)

    inserted = bulk_insert(table_name, rows)
    print(f"load_gz: {file['file_name']} -> {table_name}: inserted {inserted}")
    return inserted


def run() -> int:
    print("load_gz: truncate stg.gz_budget_lines, stg.gz_contracts, stg.gz_payments")
    truncate_table("stg.gz_budget_lines")
    truncate_table("stg.gz_contracts")
    truncate_table("stg.gz_payments")

    total = 0
    for file in fetch_raw_files("3. ГЗ"):
        lower_name = file["file_name"].lower()
        if "бюджетные строки" in lower_name:
            total += _load_file(file, "stg.gz_budget_lines", _budget_line)
        elif "контракты" in lower_name or "договора" in lower_name:
            total += _load_file(file, "stg.gz_contracts", _contract)
        elif "платеж" in lower_name:
            total += _load_file(file, "stg.gz_payments", _payment)
        else:
            print(f"load_gz: skip {file['file_name']}: unknown GZ file type")

    print(f"load_gz: done, inserted {total}")
    return total


if __name__ == "__main__":
    run()
