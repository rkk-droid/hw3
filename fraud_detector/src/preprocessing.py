"""Препроцессинг транзакций для обучения и онлайн-скоринга."""

import numpy as np
import pandas as pd

CAT_FEATURES = [
    "merch",
    "cat_id",
    "gender",
    "one_city",
    "us_state",
    "jobs",
    "daypart",
]

NUMERIC_FEATURES = [
    "amount",
    "log_amount",
    "distance_km",
    "lat_delta",
    "lon_delta",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "log_population",
    "is_night",
    "is_far",
]

FEATURE_COLUMNS = CAT_FEATURES + NUMERIC_FEATURES


def _haversine_km(lat1, lon1, lat2, lon2):
    """Расстояние по большой окружности, километры. NaN на входе даёт NaN."""
    radius_km = 6371.0
    lat1, lon1, lat2, lon2 = (np.radians(series.to_numpy(dtype=float)) for series in (lat1, lon1, lat2, lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    haversine = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 2 * radius_km * np.arcsin(np.sqrt(np.clip(haversine, 0.0, 1.0)))


def run_preproc(input_df: pd.DataFrame) -> pd.DataFrame:
    """Собирает модельные признаки из сырой транзакции.

    Читает колонки витрины: время, сумму, координаты клиента и мерчанта,
    население города и категориальные поля. Пишет фиксированный набор
    FEATURE_COLUMNS. Если во входе есть target, колонка сохраняется для
    обучения и в модель не попадает. Повторный вызов на тех же строках
    даёт тот же кадр и ничего не записывает во внешние хранилища.
    """
    frame = input_df.copy()
    event_time = pd.to_datetime(frame["transaction_time"])
    hour_int = event_time.dt.hour.fillna(0).astype(int)
    minute = event_time.dt.minute.fillna(0).astype(float)
    hour = hour_int.astype(float) + minute / 60.0
    dow = event_time.dt.dayofweek.fillna(0).astype(float)

    lat = pd.to_numeric(frame["lat"], errors="coerce")
    lon = pd.to_numeric(frame["lon"], errors="coerce")
    merchant_lat = pd.to_numeric(frame["merchant_lat"], errors="coerce")
    merchant_lon = pd.to_numeric(frame["merchant_lon"], errors="coerce")
    amount = pd.to_numeric(frame["amount"], errors="coerce").fillna(0.0).clip(lower=0.0)
    population = pd.to_numeric(frame["population_city"], errors="coerce").fillna(0.0).clip(lower=0.0)
    distance = pd.Series(_haversine_km(lat, lon, merchant_lat, merchant_lon), index=frame.index).fillna(0.0)

    features = pd.DataFrame(index=frame.index)
    for column in ("merch", "cat_id", "gender", "one_city", "us_state", "jobs"):
        values = frame[column].fillna("unknown").astype(str).str.strip()
        features[column] = values.replace({"": "unknown", "nan": "unknown", "None": "unknown"})
    features["daypart"] = np.select(
        [hour_int < 6, hour_int < 12, hour_int < 18],
        ["night", "morning", "day"],
        default="evening",
    )
    features["amount"] = amount
    features["log_amount"] = np.log1p(amount)
    features["distance_km"] = distance
    features["lat_delta"] = (merchant_lat - lat).fillna(0.0)
    features["lon_delta"] = (merchant_lon - lon).fillna(0.0)
    features["hour_sin"] = np.sin(2 * np.pi * hour / 24.0)
    features["hour_cos"] = np.cos(2 * np.pi * hour / 24.0)
    features["dow_sin"] = np.sin(2 * np.pi * dow / 7.0)
    features["dow_cos"] = np.cos(2 * np.pi * dow / 7.0)
    features["log_population"] = np.log1p(population)
    features["is_night"] = (hour_int < 6).astype(int)
    features["is_far"] = (distance > 100).astype(int)

    if "target" in frame.columns:
        features["target"] = pd.to_numeric(frame["target"], errors="coerce")
    return features
