import csv
import hashlib
import json
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://budget_user:budget_pass@postgres:5432/budget_analytics",
)

DATA_DIR = Path(os.getenv("DATA_DIR", "/data/incoming"))

engine = create_engine(DATABASE_URL)

CSV_ENCODINGS = ("utf-8-sig", "cp1251")
CSV_DELIMITERS = (",", ";", "\t", "|")


def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def read_text_with_encoding(path: Path) -> tuple[str, str]:
    raw = path.read_bytes()
    last_error = None

    for encoding in CSV_ENCODINGS:
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError as exc:
            last_error = exc

    if last_error:
        raise last_error

    return raw.decode(CSV_ENCODINGS[0]), CSV_ENCODINGS[0]


def detect_csv_delimiter(text: str) -> str:
    sample = text[:65536]

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters="".join(CSV_DELIMITERS))
        if dialect.delimiter in CSV_DELIMITERS:
            return dialect.delimiter
    except csv.Error:
        pass

    lines = [
        line
        for line in sample.splitlines()
        if line.strip()
    ][:50]

    scores = {}
    for delimiter in CSV_DELIMITERS:
        counts = [line.count(delimiter) for line in lines]
        non_zero_counts = [count for count in counts if count > 0]
        if not non_zero_counts:
            continue

        scores[delimiter] = (
            len(non_zero_counts),
            max(non_zero_counts),
            sum(non_zero_counts),
        )

    if not scores:
        return ","

    return max(scores, key=scores.get)


def read_csv_robust(path: Path) -> pd.DataFrame:
    text, encoding = read_text_with_encoding(path)
    delimiter = detect_csv_delimiter(text)
    print(f"Reading CSV: {path.name}, encoding: {encoding}, delimiter: {repr(delimiter)}")
    return pd.read_csv(path, sep=delimiter, engine="python", encoding=encoding)


def insert_import_file(path: Path, folder_name: str, hash_value: str, rows_count: int):
    with engine.begin() as conn:
        exists = conn.execute(
            text("select id from raw.import_files where file_hash = :hash"),
            {"hash": hash_value},
        ).fetchone()

        if exists:
            return None

        result = conn.execute(
            text("""
                insert into raw.import_files
                (folder_name, file_name, file_path, file_hash, file_type, rows_count, status)
                values
                (:folder_name, :file_name, :file_path, :file_hash, :file_type, :rows_count, 'loaded_raw')
                returning id
            """),
            {
                "folder_name": folder_name,
                "file_name": path.name,
                "file_path": str(path),
                "file_hash": hash_value,
                "file_type": path.suffix.lower(),
                "rows_count": rows_count,
            },
        )

        return result.scalar_one()


def load_raw_rows(import_file_id: int, df: pd.DataFrame):
    rows = []

    for i, row in df.iterrows():
        data = {
            str(k): None if pd.isna(v) else str(v)
            for k, v in row.to_dict().items()
        }
        rows.append({
            "import_file_id": import_file_id,
            "row_number": int(i),
            "data": data,
        })

    with engine.begin() as conn:
        conn.execute(
            text("""
                insert into raw.csv_rows (import_file_id, row_number, data)
                values (:import_file_id, :row_number, cast(:data as jsonb))
            """),
            [
                {
                    "import_file_id": r["import_file_id"],
                    "row_number": r["row_number"],
                    "data": json.dumps(r["data"], ensure_ascii=False),
                }
                for r in rows
            ],
        )


def main():
    print(f"DATA_DIR: {DATA_DIR.resolve()}")

    if not DATA_DIR.exists():
        print(f"DATA_DIR does not exist: {DATA_DIR}")
        return

    csv_files = sorted(
        p for p in DATA_DIR.rglob("*")
        if p.is_file() and p.suffix.lower() == ".csv"
    )

    print(f"Found CSV files: {len(csv_files)}")
    for path in csv_files:
        print(f" - {path}")

    for path in csv_files:
        folder_name = path.parent.name
        hash_value = file_hash(path)

        df = read_csv_robust(path)

        import_file_id = insert_import_file(path, folder_name, hash_value, len(df))

        if import_file_id is None:
            print(f"Skipped duplicate: {path.name}")
            continue

        load_raw_rows(import_file_id, df)
        print(f"Loaded raw: {path.name}, rows: {len(df)}")


if __name__ == "__main__":
    main()
