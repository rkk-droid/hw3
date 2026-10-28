"""Дозапись подготовленного месячного батча в размеченную витрину."""

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline.db import copy_labeled_frame, get_conn

logger = logging.getLogger(__name__)


def append_labeled_batch(batch_info: dict) -> dict:
    """Дописывает parquet батча в конец labeled_transactions.

    Читает путь и месяц из batch_info. Если этот месяц уже отмечен в
    supplied_months, строки второй раз не вставляет. COPY и маркер месяца
    фиксируются одной транзакцией, поэтому обрыв посередине не оставляет
    батч без маркера и повтор не дублирует его. Возвращает месяц и число
    новых строк, чтобы очистка истории знала, какой батч только что учитывался.
    """
    batch_path = Path(batch_info["batch_path"])
    if not batch_path.exists():
        raise FileNotFoundError(f"Нет файла батча для загрузки: {batch_path}")

    frame = pd.read_parquet(batch_path)
    batch_month = date.fromisoformat(batch_info["batch_month"])
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS supplied_months (
                    batch_month DATE PRIMARY KEY,
                    rows INT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cur.execute(
                "SELECT 1 FROM supplied_months WHERE batch_month = %s",
                (batch_month,),
            )
            already_loaded = cur.fetchone() is not None
        if already_loaded:
            conn.commit()
            logger.info("Месяц %s уже записан, повторно не дописываем.", batch_month)
            return {
                "batch_path": batch_info["batch_path"],
                "batch_month": batch_info["batch_month"],
                "rows": 0,
            }

        copy_labeled_frame(conn, frame, commit=False)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO supplied_months (batch_month, rows)
                VALUES (%s, %s)
                """,
                (batch_month, len(frame)),
            )
        conn.commit()
    finally:
        conn.close()

    logger.info("Месяц %s добавлен в витрину, строк: %s", batch_month, len(frame))
    return {
        "batch_path": batch_info["batch_path"],
        "batch_month": batch_info["batch_month"],
        "rows": int(len(frame)),
    }
