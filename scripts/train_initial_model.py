"""Обучает стартовую модель initial на окне январь–апрель 2019.

Контрольный месяц — апрель. Гиперпараметры те же, что у DAG переобучения,
чтобы гейт сравнивал сопоставимые ROC-AUC. Скрипт нужен один раз, чтобы
положить бинарник в образ скоринга. Сам сервис его не запускает.
"""

import json
import os
import sys
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import f1_score, roc_auc_score

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "fraud_detector" / "src"))
sys.path.insert(0, str(ROOT))

from preprocessing import CAT_FEATURES, run_preproc
from pipeline.retrain.model_params import CATBOOST_PARAMS, FRAUD_THRESHOLD

CSV_PATH = Path(os.environ.get("TRAIN_DATA_CSV", ""))
MODEL_PATH = ROOT / "fraud_detector" / "models" / "my_catboost.cbm"
METRICS_PATH = ROOT / "fraud_detector" / "models" / "initial.metrics.json"
WINDOW_START = "2019-01-01"
TEST_START = "2019-04-01"
WINDOW_END = "2019-05-01"


def main() -> None:
    if not CSV_PATH.exists():
        raise SystemExit("Задайте TRAIN_DATA_CSV — путь к размеченному train.csv")
    frame = pd.read_csv(CSV_PATH)
    frame["transaction_time"] = pd.to_datetime(frame["transaction_time"])
    window = frame[
        (frame["transaction_time"] >= WINDOW_START) & (frame["transaction_time"] < WINDOW_END)
    ].copy()
    train_raw = window[window["transaction_time"] < TEST_START]
    test_raw = window[window["transaction_time"] >= TEST_START]
    train = run_preproc(train_raw)
    test = run_preproc(test_raw)
    x_train = train.drop(columns=["target"])
    y_train = train["target"]
    x_test = test.drop(columns=["target"])
    y_test = test["target"]
    for column in CAT_FEATURES:
        x_train[column] = x_train[column].astype(str)
        x_test[column] = x_test[column].astype(str)

    model = CatBoostClassifier(**CATBOOST_PARAMS)
    model.fit(
        x_train,
        y_train,
        cat_features=CAT_FEATURES,
        eval_set=(x_test, y_test),
        use_best_model=True,
    )
    proba = model.predict_proba(x_test)[:, 1]
    pred = (proba > FRAUD_THRESHOLD).astype(int)
    roc_auc = float(roc_auc_score(y_test, proba))
    f1 = float(f1_score(y_test, pred, zero_division=0))
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    model.save_model(str(MODEL_PATH))
    metrics = {
        "version": "initial",
        "batch_month": "2019-04-01",
        "train_months": "2019-01,2019-02,2019-03",
        "test_month": "2019-04",
        "train_rows": int(len(x_train)),
        "test_rows": int(len(x_test)),
        "roc_auc": roc_auc,
        "f1": f1,
        "promoted": True,
        "gate_reason": "Стартовая модель, обученная до запуска DAG",
        "previous_roc_auc": None,
    }
    METRICS_PATH.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(metrics, ensure_ascii=False, indent=2))
    print("saved", MODEL_PATH)


if __name__ == "__main__":
    main()
