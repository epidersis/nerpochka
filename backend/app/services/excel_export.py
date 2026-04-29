from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from io import BytesIO
from typing import Any

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import column_index_from_string, get_column_letter
from sqlalchemy import text

from app.database import engine

SECTION_SHEETS = [
    {
        "section": "КИК",
        "sheet": "Раздел 1 (КИК)",
        "title": "Раздел 1. КИК",
        "note": "КЦСР=*****978**",
        "name_header": "Наименование мероприятия",
    },
    {
        "section": "СКК",
        "sheet": "Раздел 2 (СКК)",
        "title": "Раздел 2. Сведения о кассовых операциях за счет средств специальных казначейских кредитов (СКК)",
        "note": "КЦСР=*****6105*",
        "name_header": "Наименование мероприятия",
    },
    {
        "section": "Раздел 2/3",
        "sheet": "23",
        "title": "Раздел 3. 2/3",
        "note": "КЦСР=*****970**",
        "name_header": "Наименование мероприятия",
    },
    {
        "section": "ОКВ",
        "sheet": "ОКВ",
        "title": "Раздел 4. Объекты капитальных вложений",
        "note": "ДопКР не равен 0",
        "name_header": "Наименование объекта",
    },
]

NUMBER_FORMAT = '#,##0.00'
DATE_FORMAT = 'DD.MM.YYYY'


def _section_sql(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return f"""
        case
            when {prefix}kcsr_code ilike '%6105%' then 'СКК'
            when {prefix}kcsr_code ilike '%978%' then 'КИК'
            when {prefix}kcsr_code ilike '%970%' then 'Раздел 2/3'
            when nullif({prefix}kdr_code, '') is not null and {prefix}kdr_code not in ('0', '0.0') then 'ОКВ'
            else 'Другое'
        end
    """


def _object_key_sql(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return f"""
        replace(
            case
                when ({_section_sql(alias)}) = 'ОКВ'
                    then coalesce(nullif({prefix}kdr_code, ''), {prefix}kcsr_code)
                else {prefix}kcsr_code
            end,
            '.',
            ''
        )
    """


def _to_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    return float(value or 0)


def _first_text(*values: Any) -> str | None:
    for value in values:
        if value not in (None, ""):
            return str(value)
    return None


def _fetch_base_rows(section_filter: str | None, kcsr_code: str | None) -> dict[tuple[str, str], dict[str, Any]]:
    query = text(f"""
        select
            ({_section_sql('b')}) as section,
            ({_object_key_sql('b')}) as object_code,
            coalesce(max(nullif(b.estimate_name, '')), max(nullif(b.recipient_name, '')), ({_object_key_sql('b')})) as object_name,
            max(b.kcsr_code) as kcsr_code,
            max(b.kdr_code) as kdr_code,
            sum(b.amount) filter (where b.documentclass_id = '273') as limit_amount,
            sum(b.amount) filter (where b.documentclass_id = '313') as obligation_amount,
            sum(b.amount) filter (where b.documentclass_id = 'cash_execution') as cash_amount,
            sum(b.amount) filter (
                where b.documentclass_id = 'cash_execution'
                  and regexp_replace(coalesce(b.kvr_code, ''), '[^0-9]', '', 'g') like '2%%'
            ) as cash_contract_amount,
            sum(b.amount) filter (
                where b.documentclass_id = 'cash_execution'
                  and regexp_replace(coalesce(b.kvr_code, ''), '[^0-9]', '', 'g') like '6%%'
            ) as cash_subsidy_amount,
            sum(b.amount) filter (
                where b.documentclass_id = 'cash_execution'
                  and regexp_replace(coalesce(b.kvr_code, ''), '[^0-9]', '', 'g') like '5%%'
            ) as cash_mbt_amount
        from stg.budget_operations b
        where (:section is null or ({_section_sql('b')}) = :section)
          and (:kcsr_code is null or replace(b.kcsr_code, '.', '') = replace(:kcsr_code, '.', ''))
        group by section, object_code
        order by section, object_name
    """)

    rows: dict[tuple[str, str], dict[str, Any]] = {}
    with engine.connect() as conn:
        for row in conn.execute(query, {"section": section_filter, "kcsr_code": kcsr_code}).mappings():
            key = (row["section"], row["object_code"])
            rows[key] = {
                "section": row["section"],
                "object_code": row["object_code"],
                "object_name": row["object_name"],
                "kcsr_code": row["kcsr_code"],
                "kdr_code": row["kdr_code"],
                "limit_amount": _to_float(row["limit_amount"]),
                "obligation_amount": _to_float(row["obligation_amount"]),
                "cash_amount": _to_float(row["cash_amount"]),
                "cash_contract_amount": _to_float(row["cash_contract_amount"]),
                "cash_subsidy_amount": _to_float(row["cash_subsidy_amount"]),
                "cash_mbt_amount": _to_float(row["cash_mbt_amount"]),
            }
    return rows


def _merge_detail(target: dict[tuple[str, str], dict[str, Any]], key: tuple[str, str], prefix: str, row: dict[str, Any]) -> None:
    item = target.setdefault(key, {
        "section": key[0],
        "object_code": key[1],
        "object_name": key[1],
        "limit_amount": 0.0,
        "obligation_amount": 0.0,
        "cash_amount": 0.0,
        "cash_contract_amount": 0.0,
        "cash_subsidy_amount": 0.0,
        "cash_mbt_amount": 0.0,
    })
    item["object_name"] = _first_text(item.get("object_name"), row.get("object_name"), key[1])
    item[f"{prefix}_amount"] = item.get(f"{prefix}_amount", 0.0) + _to_float(row.get("amount"))
    item.setdefault(f"{prefix}_date", row.get("date_value"))
    item.setdefault(f"{prefix}_number", row.get("number_value"))
    item.setdefault(f"{prefix}_name", row.get("name_value"))


def _fetch_contracts(target: dict[tuple[str, str], dict[str, Any]], section_filter: str | None, kcsr_code: str | None) -> None:
    query = text(f"""
        select
            ({_section_sql('b')}) as section,
            ({_object_key_sql('b')}) as object_code,
            b.kcsr_code,
            c.con_date as date_value,
            c.con_number as number_value,
            coalesce(c.supplier_name, c.customer_name) as name_value,
            c.con_amount as amount,
            coalesce(b.kcsr_code, ({_object_key_sql('b')})) as object_name
        from stg.gz_contracts c
        join stg.gz_budget_lines b on b.con_document_id = c.con_document_id
        where (:section is null or ({_section_sql('b')}) = :section)
          and (:kcsr_code is null or replace(b.kcsr_code, '.', '') = replace(:kcsr_code, '.', ''))
          and c.con_amount is not null
    """)
    with engine.connect() as conn:
        for row in conn.execute(query, {"section": section_filter, "kcsr_code": kcsr_code}).mappings():
            _merge_detail(target, (row["section"], row["object_code"]), "contract", dict(row))


def _fetch_agreements(target: dict[tuple[str, str], dict[str, Any]], section_filter: str | None, kcsr_code: str | None) -> None:
    query = text(f"""
        select
            ({_section_sql('a')}) as section,
            ({_object_key_sql('a')}) as object_code,
            a.documentclass_id,
            a.period_to as date_value,
            a.agreement_number as number_value,
            a.recipient_name as name_value,
            a.amount,
            coalesce(a.estimate_name, a.kcsr_code, ({_object_key_sql('a')})) as object_name
        from stg.agreements a
        where (:section is null or ({_section_sql('a')}) = :section)
          and (:kcsr_code is null or replace(a.kcsr_code, '.', '') = replace(:kcsr_code, '.', ''))
          and a.amount is not null
    """)
    with engine.connect() as conn:
        for row in conn.execute(query, {"section": section_filter, "kcsr_code": kcsr_code}).mappings():
            prefix = "mbt" if row["documentclass_id"] == "273" else "subsidy"
            _merge_detail(target, (row["section"], row["object_code"]), prefix, dict(row))


def _fetch_buau(target: dict[tuple[str, str], dict[str, Any]], section_filter: str | None, kcsr_code: str | None) -> None:
    query = text(f"""
        select
            ({_section_sql('b')}) as section,
            ({_object_key_sql('b')}) as object_code,
            coalesce(b.amount_with_refund, b.amount) as amount,
            coalesce(b.organization_name, b.budget_name, b.kcsr_code, ({_object_key_sql('b')})) as object_name
        from stg.buau_operations b
        where (:section is null or ({_section_sql('b')}) = :section)
          and (:kcsr_code is null or replace(b.kcsr_code, '.', '') = replace(:kcsr_code, '.', ''))
          and coalesce(b.amount_with_refund, b.amount) is not null
    """)
    with engine.connect() as conn:
        for row in conn.execute(query, {"section": section_filter, "kcsr_code": kcsr_code}).mappings():
            item = target.setdefault((row["section"], row["object_code"]), {
                "section": row["section"],
                "object_code": row["object_code"],
                "object_name": row["object_name"] or row["object_code"],
            })
            item["buau_cash_amount"] = item.get("buau_cash_amount", 0.0) + _to_float(row["amount"])


def _load_export_data(section_filter: str | None, kcsr_code: str | None) -> dict[str, list[dict[str, Any]]]:
    items = _fetch_base_rows(section_filter, kcsr_code)
    _fetch_contracts(items, section_filter, kcsr_code)
    _fetch_agreements(items, section_filter, kcsr_code)
    _fetch_buau(items, section_filter, kcsr_code)

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items.values():
        grouped[item["section"]].append(item)

    for rows in grouped.values():
        rows.sort(key=lambda row: (row.get("object_name") or "", row.get("object_code") or ""))
    return grouped


def _set_title(ws, title: str, note: str, last_col: int) -> None:
    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=last_col)
    ws.merge_cells(start_row=3, start_column=1, end_row=3, end_column=last_col)
    ws["A2"] = title
    ws["A3"] = note
    ws["A2"].font = Font(size=13, bold=True)
    ws["A3"].font = Font(size=11, italic=True)


def _merge(ws, cell_range: str, value: str) -> None:
    ws.merge_cells(cell_range)
    cell = ws[cell_range.split(":", 1)[0]]
    cell.value = value


def _build_headers(ws, meta: dict[str, str], last_col: int) -> None:
    _set_title(ws, meta["title"], meta["note"], last_col)

    header_ranges = [
        ("A4:A7", meta["name_header"]),
        ("B4:B7", "лимит бюджета субъекта РФ"),
        ("C4:C7", "принятые бюджетные обязательства бюджета субъекта РФ\nвсего"),
        ("D4:H6", "договоры, контракты на закупку товаров, работ, услуг"),
        ("I4:M6", "межбюджетные трансферты местным бюджетам"),
        ("N4:R6", "субсидии бюджетным, автономным учреждениям (ЮЛ, ИП, ФЛ)"),
        ("S4:W6", "договоры, контракты БУ/АУ на закупку товаров, работ, услуг"),
        ("X4:AB6", "кассовые выплаты из бюджета субъекта РФ"),
        ("AC4:AG6", "местные бюджеты: лимиты и обязательства"),
        ("AH4:AL6", "местные бюджеты: контракты и субсидии"),
        ("AM4:AQ6", "кассовые выплаты из местных бюджетов"),
    ]
    if last_col >= 53:
        header_ranges.append(("AR4:BA6", "дополнительные сведения по ОКВ/БУАУ"))

    for cell_range, value in header_ranges:
        _merge(ws, cell_range, value)

    row7 = {
        "D": "итого", "E": "Дата", "F": "Номер", "G": "Контрагент", "H": "Сумма",
        "I": "итого", "J": "Дата", "K": "Номер", "L": "Получатель", "M": "Сумма",
        "N": "итого", "O": "Дата", "P": "Номер", "Q": "Получатель", "R": "Сумма",
        "S": "итого", "T": "Дата", "U": "Номер", "V": "Контрагент", "W": "Сумма",
        "X": "всего", "Y": "контракты", "Z": "субсидии БУ/АУ", "AA": "МБТ", "AB": "касса БУ/АУ",
        "AC": "лимит", "AD": "БО всего", "AE": "контракты", "AF": "субсидии", "AG": "касса БУ/АУ",
        "AH": "Дата", "AI": "Номер", "AJ": "Контрагент", "AK": "Сумма", "AL": "итого БУ/АУ",
        "AM": "всего", "AN": "контракты", "AO": "субсидии", "AP": "касса БУ/АУ", "AQ": "примечание",
        "AR": "факт поставки", "AS": "дата оплаты", "AT": "№ документа", "AU": "сумма",
        "AV": "местный лимит", "AW": "местное БО", "AX": "местная касса", "AY": "контракты",
        "AZ": "субсидии", "BA": "касса АУ/БУ",
    }
    for column, value in row7.items():
        if column_index_from_string(column) <= last_col:
            ws[column + "7"] = value

    for col in range(1, last_col + 1):
        ws.cell(8, col, col)

    source_notes = [
        "РЧБ - Наименование",
        "РЧБ - Лимиты",
        "РЧБ - Подтв. лимитов по БО",
        "ГЗ - con_amount",
        "ГЗ - con_date",
        "ГЗ - con_number",
        "ГЗ - contractor",
        "ГЗ - con_amount",
        "Соглашения МБТ - amount_1year",
        "Соглашения МБТ - date",
        "Соглашения МБТ - number",
        "Соглашения МБТ - recipient",
        "Соглашения МБТ - amount",
        "Соглашения БУ/АУ - amount",
        "Соглашения БУ/АУ - date",
        "Соглашения БУ/АУ - number",
        "Соглашения БУ/АУ - recipient",
        "Соглашения БУ/АУ - amount",
    ]
    for index, value in enumerate(source_notes, start=1):
        if index <= last_col:
            ws.cell(9, index, value)


def _apply_style(ws, last_col: int) -> None:
    thin = Side(style="thin", color="B8C2D0")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    header_fill = PatternFill("solid", fgColor="DDEBFF")
    subheader_fill = PatternFill("solid", fgColor="EAF2FF")

    widths = {
        "A": 48, "B": 18, "C": 20, "D": 16, "E": 14, "F": 16, "G": 28, "H": 16,
        "I": 16, "J": 14, "K": 16, "L": 28, "M": 16, "N": 16, "O": 14, "P": 16,
        "Q": 28, "R": 16,
    }
    for col in range(1, last_col + 1):
        letter = get_column_letter(col)
        ws.column_dimensions[letter].width = widths.get(letter, 15)

    for row in ws.iter_rows(min_row=4, max_row=9, min_col=1, max_col=last_col):
        for cell in row:
            cell.border = border
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.font = Font(bold=True, size=9)
            cell.fill = header_fill if cell.row in (4, 5, 6) else subheader_fill

    ws.freeze_panes = "A10"
    ws.auto_filter.ref = f"A9:{get_column_letter(last_col)}9"


def _write_value(ws, row: int, col: int, value: Any) -> None:
    cell = ws.cell(row, col, value)
    if isinstance(value, (int, float, Decimal)):
        cell.number_format = NUMBER_FORMAT
    elif hasattr(value, "year"):
        cell.number_format = DATE_FORMAT


def _write_data(ws, rows: list[dict[str, Any]], last_col: int) -> None:
    start_row = 10
    for offset, item in enumerate(rows):
        row = start_row + offset
        values = {
            1: item.get("object_name"),
            2: item.get("limit_amount"),
            3: item.get("obligation_amount"),
            4: item.get("contract_amount"),
            5: item.get("contract_date"),
            6: item.get("contract_number"),
            7: item.get("contract_name"),
            8: item.get("contract_amount"),
            9: item.get("mbt_amount"),
            10: item.get("mbt_date"),
            11: item.get("mbt_number"),
            12: item.get("mbt_name"),
            13: item.get("mbt_amount"),
            14: item.get("subsidy_amount"),
            15: item.get("subsidy_date"),
            16: item.get("subsidy_number"),
            17: item.get("subsidy_name"),
            18: item.get("subsidy_amount"),
            24: item.get("cash_amount"),
            25: item.get("cash_contract_amount"),
            26: item.get("cash_subsidy_amount"),
            27: item.get("cash_mbt_amount"),
            28: item.get("buau_cash_amount"),
            29: item.get("limit_amount"),
            30: item.get("obligation_amount"),
            31: item.get("contract_amount"),
            32: item.get("subsidy_amount"),
            33: item.get("buau_cash_amount"),
            34: item.get("contract_date"),
            35: item.get("contract_number"),
            36: item.get("contract_name"),
            37: item.get("contract_amount"),
            38: item.get("subsidy_amount"),
            39: item.get("cash_amount"),
            40: item.get("cash_contract_amount"),
            41: item.get("cash_subsidy_amount"),
            42: item.get("buau_cash_amount"),
            43: item.get("object_code"),
            44: None,
            45: item.get("buau_cash_amount"),
            46: item.get("limit_amount"),
            47: item.get("obligation_amount"),
            48: item.get("cash_amount"),
            49: item.get("cash_contract_amount"),
            50: item.get("cash_subsidy_amount"),
            51: item.get("buau_cash_amount"),
            52: item.get("object_code"),
            53: item.get("buau_cash_amount"),
        }
        for col, value in values.items():
            if col <= last_col and value not in (None, ""):
                _write_value(ws, row, col, value)
        for col in range(1, last_col + 1):
            cell = ws.cell(row, col)
            cell.border = Border(
                left=Side(style="thin", color="D6DEE8"),
                right=Side(style="thin", color="D6DEE8"),
                top=Side(style="thin", color="D6DEE8"),
                bottom=Side(style="thin", color="D6DEE8"),
            )
            cell.alignment = Alignment(vertical="top", wrap_text=True)


def build_analytics_export(section: str | None = None, kcsr_code: str | None = None) -> BytesIO:
    data = _load_export_data(section, kcsr_code)
    wb = Workbook()
    wb.remove(wb.active)
    wb.properties.title = "Сводная выгрузка nerpochka"
    wb.properties.created = datetime.now()

    for meta in SECTION_SHEETS:
        if section and meta["section"] != section:
            continue
        last_col = 53 if meta["section"] == "ОКВ" else 49 if meta["section"] == "Раздел 2/3" else 45
        ws = wb.create_sheet(meta["sheet"])
        _build_headers(ws, meta, last_col)
        _apply_style(ws, last_col)
        _write_data(ws, data.get(meta["section"], []), last_col)

    if not wb.worksheets:
        ws = wb.create_sheet("Нет данных")
        ws["A1"] = "Нет данных для выбранных фильтров"

    output = BytesIO()
    wb.save(output)
    output.seek(0)
    return output
