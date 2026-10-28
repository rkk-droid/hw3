"""Общие пути артефактов обучения, поставки батчей и отчётов качества."""

import os
from pathlib import Path

TRAINING_DIR = Path(os.getenv("TRAINING_DATA_DIR", "/opt/airflow/training_data"))
MODELS_DIR = Path(os.getenv("MODELS_DIR", "/opt/airflow/models"))
DQ_REPORTS_DIR = Path(os.getenv("DQ_REPORTS_DIR", "/opt/airflow/dq_reports"))
FRAUD_SRC = os.getenv("FRAUD_SRC", "/opt/airflow/fraud_src")

CURRENT_DATASET = TRAINING_DIR / "current.parquet"
PENDING_WINDOW = TRAINING_DIR / "pending_window.parquet"
BATCHES_DIR = TRAINING_DIR / "batches"
SPLITS_DIR = TRAINING_DIR / "splits"
SUPPLY_BATCH = TRAINING_DIR / "supply_batch.parquet"
PROD_METRICS = MODELS_DIR / "prod_metrics.json"

LABEL = "target"
SAMPLE_SIZE = 80_000
HISTORY_YEARS = 3
WINDOW_MONTHS = 4
