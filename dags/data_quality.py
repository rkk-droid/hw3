"""DAG проверок качества размеченной витрины.

Файл только связывает задачи. Логика лежит в pipeline.quality.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.quality.evaluate_quality_rules import evaluate_quality_rules
from pipeline.quality.profile_labeled_data import profile_labeled_data
from pipeline.quality.publish_quality_report import publish_quality_report


@dag(
    dag_id="data_quality",
    description="Раз в 5 минут проверяет витрину и публикует HTML-отчёт",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["mlops", "quality"],
    doc_md="""
### Data Quality витрины

1. Считает агрегаты по `labeled_transactions`.
2. Сверяет их с правилами: полнота, домен target, диапазон суммы, координаты, время, доля фрода, пол, код штата.
3. Кладёт HTML в `/opt/airflow/dq_reports` (на хосте это каталог `dq_reports/` рядом с `model_registry/`).

Провал правила не останавливает DAG: статус виден в отчёте. Витрина не изменяется.
    """,
)
def data_quality():
    """Связывает снимок витрины, проверку правил и публикацию HTML."""
    profile = task(profile_labeled_data, task_id="profile_labeled_data")()
    verdict = task(evaluate_quality_rules, task_id="evaluate_quality_rules")(profile)
    task(publish_quality_report, task_id="publish_quality_report")(verdict)


data_quality()
