from __future__ import annotations

import calendar
import os
import re
from datetime import date
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
    truncate_table,
)

REPORT_YEAR = int(os.getenv("REPORT_YEAR", "2025"))

HEADER_MARKERS = {"КФСР", "КЦСР", "КВР", f"Лимиты ПБС {REPORT_YEAR} год"}

AMOUNT_COLUMNS = {
    f"Лимиты ПБС {REPORT_YEAR} год": "273",
    f"Подтв. лимитов по БО {REPORT_YEAR} год": "313",
    "Всего выбытий (бух.уч.)": "cash_execution",
}


def ensure_schema() -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            alter table stg.budget_operations
            add column if not exists kdr_name text
        """))


def _period_to_from_file(file_name: str) -> date | None:
    month_map = {
        "январь": 1,
        "февраль": 2,
        "март": 3,
        "апрель": 4,
        "май": 5,
        "июнь": 6,
        "июль": 7,
        "август": 8,
        "сентябрь": 9,
        "октябрь": 10,
        "ноябрь": 11,
        "декабрь": 12,
    }
    lower_name = file_name.lower()
    year_match = re.search(r"20\d{2}", lower_name)
    if not year_match:
        return None

    year = int(year_match.group(0))
    for month_name, month in month_map.items():
        if month_name in lower_name:
            return date(year, month, calendar.monthrange(year, month)[1])
    return None


def _find_header(rows) -> tuple[int | None, dict[str, str]]:
    for row in rows:
        data = dict(row["data"])
        values = {clean_text(value) for value in data.values()}
        if len(HEADER_MARKERS.intersection(values)) >= 2:
            return row["row_number"], {
                key: clean_text(value)
                for key, value in data.items()
                if clean_text(value)
            }
    return None, {}


def _get(data: dict[str, Any], header: dict[str, str], column_name: str) -> Any:
    for raw_key, normalized_name in header.items():
        if normalized_name == column_name:
            return data.get(raw_key)
    return data.get(column_name)


def _build_rows(import_file_id: int, file_name: str, rows, header: dict[str, str], header_row: int):
    result = []
    period_to = _period_to_from_file(file_name)

    if period_to is None or period_to.year != REPORT_YEAR:
        return result

    for raw_row in rows:
        if raw_row["row_number"] <= header_row:
            continue

        data = dict(raw_row["data"])
        kcsr_code = clean_code(_get(data, header, "КЦСР"))
        kvr_code = clean_code(_get(data, header, "КВР"))
        kfsr_code = clean_code(_get(data, header, "КФСР"))
        if not any((kcsr_code, kvr_code, kfsr_code)):
            continue

        base = {
            "import_file_id": import_file_id,
            "period_from": clean_date(_get(data, header, "Дата проводки")),
            "period_to": period_to,
            "budget_id": clean_text(_get(data, header, "Бюджет")),
            "document_id": f"{import_file_id}:{raw_row['row_number']}",
            "estimate_name": clean_text(_get(data, header, "Наименование КЦСР")),
            "recipient_name": clean_text(_get(data, header, "Наименование КВСР")),
            "kadmr_code": clean_code(_get(data, header, "КВСР")),
            "kfsr_code": kfsr_code,
            "kcsr_code": kcsr_code,
            "kvr_code": kvr_code,
            "kesr_code": clean_code(_get(data, header, "КОСГУ")),
            "kdr_code": clean_code(_get(data, header, "Код цели")),
            "kdr_name": clean_text(_get(data, header, "Наименование Код цели")),
            "kde_code": clean_code(_get(data, header, "КВФО")),
            "kdf_code": clean_code(_get(data, header, "Источник средств")),
        }

        for amount_column, documentclass_id in AMOUNT_COLUMNS.items():
            amount = clean_decimal(_get(data, header, amount_column))
            if amount is None:
                continue
            result.append({
                **base,
                "documentclass_id": documentclass_id,
                "amount": amount,
            })

    return result


def run() -> int:
    print("load_rchb: ensure schema and truncate stg.budget_operations")
    ensure_schema()
    truncate_table("stg.budget_operations")

    total = 0
    for file in fetch_raw_files("1. РЧБ"):
        rows = fetch_raw_rows(file["id"])
        header_row, header = _find_header(rows)
        if header_row is None:
            print(f"load_rchb: skip {file['file_name']}: header not found")
            continue

        parsed_rows = _build_rows(file["id"], file["file_name"], rows, header, header_row)
        inserted = bulk_insert("stg.budget_operations", parsed_rows)
        total += inserted
        print(f"load_rchb: {file['file_name']}: inserted {inserted}")

    print(f"load_rchb: done, inserted {total}")
    return total


if __name__ == "__main__":
    run()
