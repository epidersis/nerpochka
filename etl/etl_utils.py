from __future__ import annotations

import os
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://budget_user:budget_pass@postgres:5432/budget_analytics",
)

engine = create_engine(DATABASE_URL)

EMPTY_VALUES = {"", "nan", "none", "null", "не указано"}


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    if value.lower() in EMPTY_VALUES:
        return None
    return value


def clean_code(value: Any) -> str | None:
    value = clean_text(value)
    if value is None:
        return None
    if re.fullmatch(r"-?\d+\.0+", value):
        return value.split(".", 1)[0]
    return value


def clean_decimal(value: Any) -> Decimal | None:
    value = clean_text(value)
    if value is None:
        return None

    normalized = (
        value.replace("\xa0", "")
        .replace(" ", "")
        .replace(",", ".")
    )
    normalized = re.sub(r"[^0-9.\-]", "", normalized)
    if normalized in {"", "-", ".", "-."}:
        return None

    try:
        return Decimal(normalized)
    except InvalidOperation:
        return None


def clean_date(value: Any) -> date | None:
    value = clean_text(value)
    if value is None:
        return None

    value = value.split(" ", 1)[0]
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    return None


def parse_period(value: Any) -> tuple[date | None, date | None]:
    value = clean_text(value)
    if value is None:
        return None, None

    dates = re.findall(r"\d{4}-\d{2}-\d{2}|\d{2}\.\d{2}\.\d{4}", value)
    if not dates:
        return None, None

    period_from = clean_date(dates[0])
    period_to = clean_date(dates[1]) if len(dates) > 1 else period_from
    return period_from, period_to


def first_value(data: dict[str, Any], names: Iterable[str]) -> Any:
    lowered = {key.lower(): key for key in data}
    for name in names:
        if name in data:
            return data[name]
        key = lowered.get(name.lower())
        if key is not None:
            return data[key]
    return None


def fetch_raw_files(folder_prefix: str):
    with engine.connect() as conn:
        return conn.execute(
            text("""
                select id, folder_name, file_name
                from raw.import_files
                where folder_name ilike :folder_prefix
                order by id
            """),
            {"folder_prefix": f"{folder_prefix}%"},
        ).mappings().all()


def fetch_raw_rows(import_file_id: int):
    with engine.connect() as conn:
        return conn.execute(
            text("""
                select row_number, data
                from raw.csv_rows
                where import_file_id = :import_file_id
                order by row_number
            """),
            {"import_file_id": import_file_id},
        ).mappings().all()


def truncate_table(table_name: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"truncate table {table_name} restart identity"))


def bulk_insert(table_name: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    columns = list(rows[0].keys())
    column_sql = ", ".join(columns)
    value_sql = ", ".join(f":{column}" for column in columns)

    with engine.begin() as conn:
        conn.execute(
            text(f"insert into {table_name} ({column_sql}) values ({value_sql})"),
            rows,
        )
    return len(rows)
