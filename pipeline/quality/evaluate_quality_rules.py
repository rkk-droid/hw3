"""Пороги data quality и вердикт по снимку витрины."""

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def _share(violations: int, rows: int) -> float:
    if rows <= 0:
        return 1.0
    return violations / rows


def _rule(rule_id: str, title: str, passed: bool, observed: str, threshold: str) -> dict:
    return {
        "rule_id": rule_id,
        "title": title,
        "passed": passed,
        "observed": observed,
        "threshold": threshold,
    }


def evaluate_quality_rules(profile: dict) -> dict:
    """Применяет правила качества к агрегатам и не меняет витрину.

    Читает словарь счётчиков из profile_labeled_data. Пишет только вердикт
    в возвращаемом словаре: число нарушений сравнивается с порогом.
    Провал правила не роняет задачу, чтобы отчёт публиковался и при красных
    проверках. Повтор на том же снимке даёт тот же вердикт.
    """
    rows = int(profile["rows"])
    fraud_rate = (int(profile["fraud_rows"]) / rows) if rows else 0.0
    required_share = _share(int(profile["required_nulls"]), rows)
    amount_share = _share(int(profile["bad_amount"]), rows)
    geo_share = _share(int(profile["bad_geo"]), rows)
    gender_share = _share(int(profile["bad_gender"]), rows)
    state_share = _share(int(profile["bad_state"]), rows)

    rules = [
        _rule(
            "required_fields_complete",
            "Обязательные поля заполнены",
            rows > 0 and required_share <= 0.001,
            f"{profile['required_nulls']} строк с пустым временем, суммой, target, merch или cat_id ({required_share:.4%})",
            "доля пустых значений не больше 0.1%, витрина не пустая",
        ),
        _rule(
            "target_binary",
            "target только 0 или 1",
            rows > 0 and int(profile["bad_target"]) == 0,
            f"{profile['bad_target']} строк вне домена {{0, 1}}",
            "ноль нарушений",
        ),
        _rule(
            "amount_in_range",
            "Сумма в допустимом диапазоне",
            rows > 0 and amount_share <= 0.001,
            f"{profile['bad_amount']} строк с amount <= 0 или amount > 100000 ({amount_share:.4%})",
            "доля выбросов не больше 0.1%",
        ),
        _rule(
            "coordinates_in_range",
            "Координаты клиента и мерчанта в диапазоне",
            rows > 0 and geo_share <= 0.001,
            f"{profile['bad_geo']} строк с широтой вне [-90, 90] или долготой вне [-180, 180] ({geo_share:.4%})",
            "доля нарушений не больше 0.1%",
        ),
        _rule(
            "event_time_plausible",
            "Время транзакции в правдоподобном диапазоне",
            rows > 0 and int(profile["bad_time"]) == 0,
            f"{profile['bad_time']} строк вне [2018-01-01, 2100-01-01)",
            "ноль нарушений",
        ),
        _rule(
            "fraud_rate_in_band",
            "Доля фрода в ожидаемом коридоре",
            rows > 0 and 0.001 <= fraud_rate <= 0.05,
            f"{fraud_rate:.4%} ({profile['fraud_rows']} из {rows})",
            "от 0.1% до 5%",
        ),
        _rule(
            "gender_domain",
            "Пол в домене F/M",
            rows > 0 and gender_share <= 0.005,
            f"{profile['bad_gender']} строк с gender вне {{F, M}} ({gender_share:.4%})",
            "доля посторонних значений не больше 0.5%",
        ),
        _rule(
            "state_code_format",
            "Штат — код из двух заглавных букв",
            rows > 0 and state_share <= 0.01,
            f"{profile['bad_state']} строк, где us_state не совпал с ^[A-Z]{{2}}$ ({state_share:.4%})",
            "доля нарушений не больше 1%",
        ),
    ]
    failed = [rule["rule_id"] for rule in rules if not rule["passed"]]
    logger.info("DQ: %s правил, провалено %s", len(rules), len(failed))
    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "rows": rows,
        "rules": rules,
        "failed_rules": failed,
    }
