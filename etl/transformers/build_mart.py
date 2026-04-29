from __future__ import annotations

from sqlalchemy import text

from etl_utils import engine

SECTION_SQL = """
    case
        when kcsr_code ilike '%6105%' then 'СКК'
        when kcsr_code ilike '%978%' then 'КИК'
        when kcsr_code ilike '%970%' then 'Раздел 2/3'
        when nullif(kdr_code, '') is not null and kdr_code not in ('0', '0.0') then 'ОКВ'
        else 'Другое'
    end
"""


def run() -> int:
    print("build_mart: truncate mart.indicators")
    with engine.begin() as conn:
        conn.execute(text("truncate table mart.indicators restart identity"))

        rchb_count = conn.execute(text(f"""
            insert into mart.indicators (
                source_type, source_file_id, period_from, period_to,
                section, object_code, object_name,
                kcsr_code, kvr_code, kfsr_code, kadmr_code, kesr_code, kdr_code, kde_code, kdf_code,
                indicator_type, amount, document_id, recipient_name
            )
            select
                'RCHB',
                import_file_id,
                period_from,
                period_to,
                {SECTION_SQL},
                kcsr_code,
                coalesce(estimate_name, recipient_name),
                kcsr_code,
                kvr_code,
                kfsr_code,
                kadmr_code,
                kesr_code,
                kdr_code,
                kde_code,
                kdf_code,
                case
                    when documentclass_id = '273' then 'limit'
                    when documentclass_id = '313' then 'budget_obligation'
                    when documentclass_id = 'cash_execution' then 'cash_execution'
                    else 'budget_amount'
                end,
                amount,
                document_id,
                recipient_name
            from stg.budget_operations
            where amount is not null
        """)).rowcount
        print(f"build_mart: RCHB indicators inserted {rchb_count}")

        contract_count = conn.execute(text(f"""
            insert into mart.indicators (
                source_type, source_file_id, period_to,
                section, object_code, object_name,
                kcsr_code, kvr_code, kfsr_code, kesr_code, kdr_code, kde_code, kdf_code,
                indicator_type, amount, contract_id, contractor_name
            )
            select
                'GZ',
                c.import_file_id,
                c.con_date,
                {SECTION_SQL},
                b.kcsr_code,
                c.con_number,
                b.kcsr_code,
                b.kvr_code,
                b.kfsr_code,
                b.kesr_code,
                b.kdr_code,
                b.kde_code,
                b.kdf_code,
                'contract_amount',
                c.con_amount,
                c.con_document_id,
                c.supplier_name
            from stg.gz_contracts c
            join stg.gz_budget_lines b on b.con_document_id = c.con_document_id
            where c.con_amount is not null
        """)).rowcount
        print(f"build_mart: GZ contract indicators inserted {contract_count}")

        payment_count = conn.execute(text(f"""
            insert into mart.indicators (
                source_type, source_file_id, period_to,
                section, object_code, object_name,
                kcsr_code, kvr_code, kfsr_code, kesr_code, kdr_code, kde_code, kdf_code,
                indicator_type, amount, contract_id, payment_id
            )
            select
                'GZ',
                p.import_file_id,
                p.payment_date,
                {SECTION_SQL},
                b.kcsr_code,
                p.payment_id,
                b.kcsr_code,
                b.kvr_code,
                b.kfsr_code,
                b.kesr_code,
                b.kdr_code,
                b.kde_code,
                b.kdf_code,
                'contract_payment',
                p.payment_amount,
                p.con_document_id,
                p.payment_id
            from stg.gz_payments p
            join stg.gz_budget_lines b on b.con_document_id = p.con_document_id
            where p.payment_amount is not null
        """)).rowcount
        print(f"build_mart: GZ payment indicators inserted {payment_count}")

    total = (rchb_count or 0) + (contract_count or 0) + (payment_count or 0)
    print(f"build_mart: done, inserted {total}")
    return total


if __name__ == "__main__":
    run()
