"""Сравнение кандидата с моделью, которая сейчас отвечает в проде."""

import json
import logging

from pipeline.settings import PROD_METRICS

logger = logging.getLogger(__name__)


def decide_promotion(candidate_auc, prod_metrics: dict | None) -> tuple[bool, str]:
    """Решает, можно ли переключить прод на кандидата.

    Единственный блокирующий критерий — ROC-AUC на самом свежем месяце окна.
    F1 при пороге 0.5 на доле фрода около 0.6% сильно прыгает между месяцами
    и в гейт не входит: метрика пишется в отчёт, но релиз по ней не режется.
    Кандидат хуже прода, если его ROC-AUC строго ниже. Равенство считается
    не хуже. Нет базовой метрики — кандидата можно ставить первым в прод.
    Неопределённый ROC-AUC в прод не пускает.
    """
    if candidate_auc is None:
        return False, "ROC-AUC на контрольном месяце не определён, в прод не берём"
    candidate = float(candidate_auc)
    if not prod_metrics or prod_metrics.get("roc_auc") is None:
        return True, "Базовой метрики прода нет, кандидат становится первой продовой моделью"
    previous = float(prod_metrics["roc_auc"])
    if candidate < previous:
        return (
            False,
            f"ROC-AUC {candidate:.4f} ниже прода {previous:.4f}, файл сохранён без переключения latest.txt",
        )
    return True, f"ROC-AUC {candidate:.4f} не ниже прода {previous:.4f}"


def evaluate_candidate(train_info: dict) -> dict:
    """Сравнивает ROC-AUC кандидата с prod_metrics.json и не трогает прод.

    Читает метрики из train_info и, если файл есть, текущие продовые метрики.
    Прод, latest.txt и training_state не меняет: решение только дописывается
    в словарь для задачи публикации. Повтор читает те же файлы. Пока публикация
    не перезаписала prod_metrics.json, решение не меняется.
    """
    prod_metrics = None
    if PROD_METRICS.exists():
        prod_metrics = json.loads(PROD_METRICS.read_text(encoding="utf-8"))
    promote, reason = decide_promotion(train_info.get("roc_auc"), prod_metrics)
    previous_auc = None if not prod_metrics else prod_metrics.get("roc_auc")
    logger.info("Гейт модели %s: promote=%s. %s", train_info.get("version"), promote, reason)
    decision = dict(train_info)
    decision["promote"] = promote
    decision["gate_reason"] = reason
    decision["previous_roc_auc"] = previous_auc
    return decision
