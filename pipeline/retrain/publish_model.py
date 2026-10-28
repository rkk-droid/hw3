"""Сохранение бинарника модели и фиксация окна после успешной записи."""

import json
import logging
import os
from datetime import date
from pathlib import Path

from pipeline.db import advance_training_state, get_conn
from pipeline.settings import CURRENT_DATASET, MODELS_DIR, PROD_METRICS

logger = logging.getLogger(__name__)


def _metrics_payload(train_info: dict) -> dict:
    return {
        "version": train_info["version"],
        "batch_month": train_info["batch_month"],
        "train_months": train_info["train_months"],
        "test_month": train_info["test_month"],
        "train_rows": train_info["train_rows"],
        "test_rows": train_info["test_rows"],
        "roc_auc": train_info["roc_auc"],
        "f1": train_info["f1"],
        "promoted": bool(train_info["promote"]),
        "gate_reason": train_info["gate_reason"],
        "previous_roc_auc": train_info.get("previous_roc_auc"),
    }


def _atomic_write(path: Path, text: str) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def publish_model(train_info: dict) -> str:
    """Кладёт файл модели в общий каталог и только потом сдвигает указатель месяца.

    Читает черновик .cbm и решение гейта из train_info. Всегда переименовывает
    черновик в my_catboost_<version>.cbm и пишет metrics json. latest.txt и
    prod_metrics.json обновляются только если гейт разрешил раскатку: модель
    хуже прода остаётся файлом в каталоге, но скоринг её не подхватывает.
    Окно current.parquet и training_state меняются после появления файла модели.
    Повтор с тем же batch_month не сдвигает окно второй раз: границы состояния
    выставляются абсолютными, а pending-файл переносится один раз.
    Возвращает id версии, файл которой сохранён.
    """
    version = train_info["version"]
    promote = bool(train_info["promote"])
    staging_model = Path(train_info["staging_model"])
    final_model = MODELS_DIR / f"my_catboost_{version}.cbm"
    metrics_path = MODELS_DIR / f"my_catboost_{version}.metrics.json"
    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    if staging_model.exists():
        os.replace(staging_model, final_model)
    elif not final_model.exists():
        raise FileNotFoundError(f"Нет файла модели для сохранения: {staging_model}")

    payload = _metrics_payload(train_info)
    _atomic_write(metrics_path, json.dumps(payload, ensure_ascii=False, indent=2))
    logger.info("Файл модели сохранён: %s", final_model)

    if promote:
        latest_tmp = MODELS_DIR / f".latest.{version}.txt"
        latest_tmp.write_text(version + "\n", encoding="utf-8")
        os.replace(latest_tmp, MODELS_DIR / "latest.txt")
        _atomic_write(PROD_METRICS, json.dumps(payload, ensure_ascii=False, indent=2))
        logger.info("Модель %s переключена в прод", version)
    else:
        logger.info("Модель %s не раскатана в прод: %s", version, train_info["gate_reason"])

    pending_window = Path(train_info["dataset_path"])
    if pending_window.exists():
        os.replace(pending_window, CURRENT_DATASET)
        logger.info("Окно обучения обновлено: %s", CURRENT_DATASET)
    elif not CURRENT_DATASET.exists():
        raise FileNotFoundError(f"Нет нового окна обучения: {pending_window}")
    else:
        logger.info("Окно уже перенесено в %s, повторно не сдвигаем.", CURRENT_DATASET)

    batch_month = date.fromisoformat(train_info["batch_month"])
    conn = get_conn()
    try:
        advance_training_state(conn, batch_month)
    finally:
        conn.close()
    return version
