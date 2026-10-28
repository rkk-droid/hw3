"""Гиперпараметры CatBoost, общие для стартовой модели и DAG переобучения."""

CATBOOST_PARAMS = {
    "iterations": 100,
    "depth": 6,
    "learning_rate": 0.1,
    "loss_function": "Logloss",
    "eval_metric": "AUC",
    "auto_class_weights": "Balanced",
    "early_stopping_rounds": 20,
    "random_seed": 42,
    "allow_writing_files": False,
    "verbose": 20,
}

FRAUD_THRESHOLD = 0.5
