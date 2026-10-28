import logging
import os
import shutil
import threading
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier


logger = logging.getLogger(__name__)

THRESHOLD = 0.5
MODELS_DIR = Path(os.getenv("MODELS_DIR", "/models"))
BUNDLED_MODEL = Path(os.getenv("BUNDLED_MODEL_PATH", "/app/models/my_catboost.cbm"))
BUNDLED_METRICS = Path(os.getenv("BUNDLED_METRICS_PATH", "/app/models/initial.metrics.json"))

_lock = threading.Lock()
_model = None
_version = None


def _model_path(version: str) -> Path:
    return MODELS_DIR / f"my_catboost_{version}.cbm"


def _copy_atomic(source: Path, dest: Path) -> None:
    """Копирует файл через временное имя, чтобы обрыв не оставил битый бинарник."""
    temporary = dest.with_suffix(dest.suffix + ".partial")
    shutil.copyfile(source, temporary)
    os.replace(temporary, dest)


def ensure_bootstrap():
    """Кладёт зашитую модель в общий каталог, пока DAG не переключит прод.

    Если latest.txt уже указывает на другую версию, файл initial не затирает её.
    Базовые метрики копируются только для версии initial и только если своего
    prod_metrics.json ещё нет: повторный старт не откатывает гейт.
    """
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    latest = MODELS_DIR / "latest.txt"
    version = latest.read_text(encoding="utf-8").strip() if latest.exists() else ""
    if not version:
        dest = _model_path("initial")
        if not dest.exists():
            if not BUNDLED_MODEL.exists():
                raise FileNotFoundError(f"Нет исходной модели: {BUNDLED_MODEL}")
            _copy_atomic(BUNDLED_MODEL, dest)
            logger.info("Скопирована исходная модель в %s", dest)
        latest.write_text("initial\n", encoding="utf-8")
        version = "initial"
    prod_metrics = MODELS_DIR / "prod_metrics.json"
    if version == "initial" and not prod_metrics.exists() and BUNDLED_METRICS.exists():
        _copy_atomic(BUNDLED_METRICS, prod_metrics)
        logger.info("Скопированы базовые метрики в %s", prod_metrics)


def _read_latest_version() -> str:
    ensure_bootstrap()
    version = (MODELS_DIR / "latest.txt").read_text(encoding="utf-8").strip()
    if not version:
        raise RuntimeError(f"Пустой указатель модели: {MODELS_DIR / 'latest.txt'}")
    return version


def _load_model(version: str) -> CatBoostClassifier:
    path = _model_path(version)
    if not path.exists():
        raise FileNotFoundError(f"Файл модели {version} не найден: {path}")
    os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
    model = CatBoostClassifier()
    model.load_model(str(path))
    logger.info("Загружена модель версии %s из %s", version, path)
    return model


def _current_model():
    """Возвращает модель, которая соответствует latest.txt. Вызывать под _lock."""
    global _model, _version
    version = _read_latest_version()
    if _model is None or version != _version:
        _model = _load_model(version)
        _version = version
    return _model, _version


def make_pred(dt, source_info="kafka"):
    with _lock:
        model, model_version = _current_model()
        features = list(model.feature_names_)
        missing = [column for column in features if column not in dt.columns]
        if missing:
            raise ValueError(f"Во входных данных нет признаков модели: {missing}")
        frame = dt[features].copy()
        for idx in model.get_cat_feature_indices():
            column = features[idx]
            if not pd.api.types.is_numeric_dtype(frame[column]):
                frame[column] = frame[column].astype(str)
        y_proba = model.predict_proba(frame)[:, 1]

    submission = pd.DataFrame({
        "score": y_proba,
        "fraud_flag": (y_proba > THRESHOLD) * 1,
    })
    logger.info("Prediction complete for data from %s, model_version=%s", source_info, model_version)
    return submission, y_proba, model_version
