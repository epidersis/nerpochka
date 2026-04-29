import os
import hashlib
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://budget_user:budget_pass@postgres:5432/budget_analytics"
)

DATA_DIR = Path(os.getenv("DATA_DIR", "/data/incoming"))

engine = create_engine(DATABASE_URL)


def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def insert_import_file(path: Path, folder_name: str, hash_value: str, rows_count: int):
    with engine.begin() as conn:
        exists = conn.execute(
            text("select id from raw.import_files where file_hash = :hash"),
            {"hash": hash_value}
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
            }
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
                    "data": pd.io.json.dumps(r["data"], force_ascii=False),
                }
                for r in rows
            ]
        )


def main():
    csv_files = [
        p for p in DATA_DIR.rglob("*")
        if p.is_file() and p.suffix.lower() == ".csv"
    ]

    print(f"Найдено CSV: {len(csv_files)}")

    for path in csv_files:
        folder_name = path.parent.name
        hash_value = file_hash(path)

        try:
            df = pd.read_csv(path, sep=None, engine="python",
                             encoding="utf-8-sig")
        except UnicodeDecodeError:
            df = pd.read_csv(path, sep=None, engine="python",
                             encoding="cp1251")

        import_file_id = insert_import_file(
            path, folder_name, hash_value, len(df))

        if import_file_id is None:
            print(f"Пропущен дубль: {path.name}")
            continue

        load_raw_rows(import_file_id, df)
        print(f"Загружен raw: {path.name}, строк: {len(df)}")


if __name__ == "__main__":
    main()
