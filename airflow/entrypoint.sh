#!/usr/bin/env bash
set -euo pipefail

mkdir -p \
  /opt/airflow/models \
  /opt/airflow/dq_reports \
  /opt/airflow/training_data \
  /opt/airflow/airflow_state \
  /opt/airflow/logs \
  /opt/airflow/dags

# Каталоги общие с data_loader и fraud_detector, которые работают от root.
chmod 777 \
  /opt/airflow/models \
  /opt/airflow/dq_reports \
  /opt/airflow/training_data \
  /opt/airflow/airflow_state \
  /opt/airflow/logs

# Стартовая модель нужна гейту и скорингу до первого успешного прогона DAG.
if [[ ! -s /opt/airflow/models/latest.txt && -f /opt/airflow/bundled/my_catboost.cbm ]]; then
  cp /opt/airflow/bundled/my_catboost.cbm /opt/airflow/models/my_catboost_initial.cbm.partial
  mv /opt/airflow/models/my_catboost_initial.cbm.partial /opt/airflow/models/my_catboost_initial.cbm
  printf 'initial\n' > /opt/airflow/models/latest.txt
fi
if [[ ! -f /opt/airflow/models/prod_metrics.json && -f /opt/airflow/bundled/initial.metrics.json ]]; then
  if [[ -f /opt/airflow/models/latest.txt ]] && [[ "$(tr -d '[:space:]' < /opt/airflow/models/latest.txt)" == "initial" ]]; then
    cp /opt/airflow/bundled/initial.metrics.json /opt/airflow/models/prod_metrics.json.partial
    mv /opt/airflow/models/prod_metrics.json.partial /opt/airflow/models/prod_metrics.json
  fi
fi

export PYTHONPATH="/opt/airflow${PYTHONPATH:+:$PYTHONPATH}"
export AIRFLOW__CORE__EXECUTOR="${AIRFLOW__CORE__EXECUTOR:-SequentialExecutor}"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:-sqlite:////opt/airflow/airflow_state/airflow.db}"
export AIRFLOW__CORE__LOAD_EXAMPLES="${AIRFLOW__CORE__LOAD_EXAMPLES:-false}"
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION="${AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION:-false}"

AIRFLOW_USER_NAME="${_AIRFLOW_WWW_USER_USERNAME:-admin}"
AIRFLOW_USER_PASSWORD="${_AIRFLOW_WWW_USER_PASSWORD:-admin}"

# В образе нет gosu: официальный entrypoint не понижает права, контейнер стартует от root.
run_as_airflow() {
  /usr/sbin/runuser --preserve-environment --user airflow -- "$@"
}

# Официальный entrypoint создаёт пользователя при каждом старте и падает, если он уже есть.
unset _AIRFLOW_DB_MIGRATE
unset _AIRFLOW_WWW_USER_CREATE

run_as_airflow airflow db migrate
run_as_airflow airflow users create \
  --username "${AIRFLOW_USER_NAME}" \
  --password "${AIRFLOW_USER_PASSWORD}" \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  || true

exec /usr/sbin/runuser --preserve-environment --user airflow -- /entrypoint standalone
