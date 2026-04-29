from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
from app.database import fetch_all
from app.services.excel_export import build_analytics_export

router = APIRouter()


@router.get("/summary")
def get_summary(
    section: str | None = None,
    kcsr_code: str | None = None,
    indicator_type: str | None = None,
    period_to: str | None = None,
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
          and (:period_to is null or period_to = cast(:period_to as date))
        group by section, kcsr_code, object_name, indicator_type
        order by section, kcsr_code, indicator_type
        limit 500
    """

    return fetch_all(query, {
        "section": section,
        "kcsr_code": kcsr_code,
        "indicator_type": indicator_type,
        "period_to": period_to,
    })


@router.get("/sections")
def get_sections():
    query = """
        select distinct section
        from mart.indicators
        where section is not null
        order by section
    """
    return fetch_all(query)


@router.get("/objects")
def get_objects(
    section: str | None = None,
    q: str | None = Query(default=None),
):
    query = """
        select distinct
            object_code,
            object_name,
            kcsr_code
        from mart.indicators
        where (:section is null or section = :section)
          and (
              :q is null
              or object_code ilike '%' || :q || '%'
              or object_name ilike '%' || :q || '%'
              or kcsr_code ilike '%' || :q || '%'
          )
        order by object_name, object_code, kcsr_code
        limit 500
    """
    return fetch_all(query, {"section": section, "q": q})


@router.get("/indicators")
def get_indicators():
    query = """
        select distinct indicator_type
        from mart.indicators
        where indicator_type is not null
        order by indicator_type
    """
    return fetch_all(query)


@router.get("/export")
def export_analytics(
    section: str | None = None,
    kcsr_code: str | None = None,
):
    output = build_analytics_export(section=section, kcsr_code=kcsr_code)
    filename = "nerpochka_analytics_export.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )
