from fastapi import APIRouter, Query
from app.database import fetch_all

router = APIRouter()


@router.get("/summary")
def get_summary(
    section: str | None = None,
    kcsr_code: str | None = None,
    indicator_type: str | None = None,
):
    query = """
        select
            section,
            kcsr_code,
            object_name,
            indicator_type,
            sum(amount) as amount
        from mart.indicators
        where (:section is null or section = :section)
          and (:kcsr_code is null or kcsr_code = :kcsr_code)
          and (:indicator_type is null or indicator_type = :indicator_type)
        group by section, kcsr_code, object_name, indicator_type
        order by section, kcsr_code, indicator_type
        limit 500
    """

    return fetch_all(query, {
        "section": section,
        "kcsr_code": kcsr_code,
        "indicator_type": indicator_type,
    })
