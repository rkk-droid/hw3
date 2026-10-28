"""Снимок агрегатов витрины labeled_transactions для проверок качества."""

import logging

from pipeline.db import get_conn

logger = logging.getLogger(__name__)

PROFILE_SQL = """
SELECT
    COUNT(*)::bigint AS rows,
    COUNT(*) FILTER (
        WHERE transaction_time IS NULL
           OR amount IS NULL
           OR target IS NULL
           OR merch IS NULL OR btrim(merch) = ''
           OR cat_id IS NULL OR btrim(cat_id) = ''
    )::bigint AS required_nulls,
    COUNT(*) FILTER (
        WHERE target IS NOT NULL AND target NOT IN (0, 1)
    )::bigint AS bad_target,
    COUNT(*) FILTER (
        WHERE amount IS NOT NULL AND (amount <= 0 OR amount > 100000)
    )::bigint AS bad_amount,
    COUNT(*) FILTER (
        WHERE lat IS NULL OR lat NOT BETWEEN -90 AND 90
           OR lon IS NULL OR lon NOT BETWEEN -180 AND 180
           OR merchant_lat IS NULL OR merchant_lat NOT BETWEEN -90 AND 90
           OR merchant_lon IS NULL OR merchant_lon NOT BETWEEN -180 AND 180
    )::bigint AS bad_geo,
    COUNT(*) FILTER (
        WHERE transaction_time IS NULL
           OR transaction_time < TIMESTAMP '2018-01-01'
           OR transaction_time >= TIMESTAMP '2100-01-01'
    )::bigint AS bad_time,
    COUNT(*) FILTER (WHERE target = 1)::bigint AS fraud_rows,
    COUNT(*) FILTER (
        WHERE gender IS NOT NULL AND gender NOT IN ('F', 'M')
    )::bigint AS bad_gender,
    COUNT(*) FILTER (
        WHERE us_state IS NOT NULL AND us_state !~ '^[A-Z]{2}$'
    )::bigint AS bad_state
FROM labeled_transactions
"""


def profile_labeled_data() -> dict:
    """Считает агрегаты витрины и возвращает их следующим задачам.

    Читает всю таблицу `labeled_transactions` одним запросом с FILTER.
    Строки не изменяет и не удаляет. Повторный запуск пересчитывает снимок
    по текущему состоянию витрины и не оставляет частичных записей.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(PROFILE_SQL)
            row = cur.fetchone()
            columns = [desc[0] for desc in cur.description]
    finally:
        conn.close()

    profile = {column: int(value) for column, value in zip(columns, row)}
    logger.info("Снимок витрины: %s строк, фродовых %s", profile["rows"], profile["fraud_rows"])
    return profile
