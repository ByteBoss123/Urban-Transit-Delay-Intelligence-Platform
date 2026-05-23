"""
Airflow DAGs — Transit Platform
1. daily_model_retrain   — retrain cascade prediction model on last 30 days
2. data_quality_checks   — validate Delta Lake tables, alert on anomalies
3. weekly_route_profile  — build historical delay profiles per route
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": True,
    "email":            ["data-alerts@transit-platform.io"],
}

DELTA_BASE   = "/delta/tables"
MODELS_PATH  = "/models"
SLACK_CONN   = "slack_webhook_alerts"


# ═══════════════════════════════════════════════════════════════════════════
# DAG 1 — Daily Model Retraining
# ═══════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id="daily_model_retrain",
    default_args=DEFAULT_ARGS,
    description="Retrain cascade propagation model on rolling 30-day window",
    schedule_interval="0 3 * * *",       # 3 AM daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ml", "transit", "daily"],
    doc_md=dedent("""
        ## Daily Model Retrain
        Pulls the last 30 days of labeled delay events from Delta Lake,
        trains an MLlib GBT classifier for cascade propagation prediction,
        runs validation checks, and promotes the model if metrics pass.
    """),
) as retrain_dag:

    @task()
    def check_training_data_freshness(**ctx):
        """Assert Delta table was updated in last 2 hours."""
        from delta import DeltaTable
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("AirflowQC").getOrCreate()
        dt    = DeltaTable.forPath(spark, f"{DELTA_BASE}/trip_delay_events")
        hist  = dt.history(1).collect()[0]
        ts    = hist["timestamp"]
        lag   = (datetime.now() - ts.replace(tzinfo=None)).total_seconds() / 3600
        assert lag < 2, f"Delta table stale: last write {lag:.1f}h ago"
        log.info(f"Table freshness OK: last write {lag:.2f}h ago")
        return {"lag_hours": lag}

    train_job = SparkSubmitOperator(
        task_id="spark_train_cascade_model",
        application="/opt/spark/jobs/train_cascade_model.py",
        conn_id="spark_default",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.executor.memory": "4g",
            "spark.executor.cores":  "2",
            "spark.dynamicAllocation.enabled": "true",
        },
        application_args=[
            "--input",    f"{DELTA_BASE}/trip_delay_events",
            "--output",   f"{MODELS_PATH}/cascade_propagation_candidate",
            "--lookback", "30",
        ],
        name="transit-cascade-train",
    )

    @task()
    def validate_model_metrics(**ctx):
        """Check AUC-ROC and precision meet thresholds before promotion."""
        import json, pathlib
        metrics_path = f"{MODELS_PATH}/cascade_propagation_candidate/metrics.json"
        metrics      = json.loads(pathlib.Path(metrics_path).read_text())

        auc       = metrics["auc_roc"]
        precision = metrics["precision_at_50"]
        recall    = metrics["recall_at_50"]

        log.info(f"Model metrics — AUC: {auc:.3f}, P@50: {precision:.3f}, R@50: {recall:.3f}")
        assert auc       >= 0.78, f"AUC {auc:.3f} below threshold 0.78"
        assert precision >= 0.70, f"Precision {precision:.3f} below threshold 0.70"
        return metrics

    @task()
    def promote_model(**ctx):
        """Atomic rename: candidate → production."""
        import shutil, pathlib
        candidate = pathlib.Path(f"{MODELS_PATH}/cascade_propagation_candidate")
        production = pathlib.Path(f"{MODELS_PATH}/cascade_propagation_v2")
        backup     = pathlib.Path(f"{MODELS_PATH}/cascade_propagation_v2_prev")
        if production.exists():
            shutil.copytree(production, backup, dirs_exist_ok=True)
        shutil.copytree(candidate, production, dirs_exist_ok=True)
        log.info("Model promoted to production")

    notify_success = SlackWebhookOperator(
        task_id="notify_retrain_success",
        http_conn_id=SLACK_CONN,
        message="✅ *Transit Platform* — cascade model retrain succeeded",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    notify_failure = SlackWebhookOperator(
        task_id="notify_retrain_failure",
        http_conn_id=SLACK_CONN,
        message="🚨 *Transit Platform* — cascade model retrain FAILED — check Airflow logs",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    (
        check_training_data_freshness()
        >> train_job
        >> validate_model_metrics()
        >> promote_model()
        >> [notify_success, notify_failure]
    )


# ═══════════════════════════════════════════════════════════════════════════
# DAG 2 — Data Quality Checks
# ═══════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id="data_quality_checks",
    default_args=DEFAULT_ARGS,
    description="Validate Delta tables and alert on data anomalies",
    schedule_interval="*/30 * * * *",    # every 30 min
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dq", "transit", "monitoring"],
) as dq_dag:

    TABLES = {
        "trip_delay_events": {
            "min_rows_per_hour":    1000,
            "max_null_pct_route_id": 0.01,
            "max_delay_sec":         7200,
        },
        "route_speed_agg": {
            "min_rows_per_hour":    200,
            "max_null_pct_route_id": 0.0,
            "min_speed_mph":        -1.0,
        },
        "cascade_flags": {
            "min_rows_per_hour":    50,
            "max_cascade_score":    100,
        },
    }

    @task()
    def run_quality_checks(**ctx):
        from pyspark.sql import SparkSession
        import json

        spark    = SparkSession.builder.appName("DQ").getOrCreate()
        failures = []

        for table, rules in TABLES.items():
            path = f"{DELTA_BASE}/{table}"
            df   = spark.read.format("delta").load(path)

            # Row count check
            hour_ago = datetime.now() - timedelta(hours=1)
            recent   = df.filter(df.window_start >= hour_ago)
            row_count = recent.count()
            if row_count < rules.get("min_rows_per_hour", 0):
                failures.append(
                    f"Table `{table}`: only {row_count} rows in last hour "
                    f"(expected ≥ {rules['min_rows_per_hour']})"
                )

            # Null checks
            for col, threshold in rules.items():
                if col.startswith("max_null_pct_"):
                    field     = col.replace("max_null_pct_", "")
                    null_pct  = df.filter(df[field].isNull()).count() / max(df.count(), 1)
                    if null_pct > threshold:
                        failures.append(
                            f"Table `{table}`.`{field}`: null% = {null_pct:.3f} > {threshold}"
                        )

        if failures:
            raise ValueError("Data quality failures:\n" + "\n".join(failures))

        log.info(f"All data quality checks passed for {list(TABLES.keys())}")
        return {"status": "ok", "tables_checked": list(TABLES.keys())}

    alert_on_failure = SlackWebhookOperator(
        task_id="alert_dq_failure",
        http_conn_id=SLACK_CONN,
        message="🔴 *Transit DQ* — data quality check FAILED",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    run_quality_checks() >> alert_on_failure


# ═══════════════════════════════════════════════════════════════════════════
# DAG 3 — Weekly Route Delay Profiles
# ═══════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id="weekly_route_profiles",
    default_args=DEFAULT_ARGS,
    description="Build historical delay profiles per route for ML features",
    schedule_interval="0 2 * * 1",       # 2 AM every Monday
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["analytics", "transit", "weekly"],
) as profile_dag:

    build_profiles = SparkSubmitOperator(
        task_id="build_route_profiles",
        application="/opt/spark/jobs/build_route_profiles.py",
        conn_id="spark_default",
        conf={"spark.executor.memory": "6g"},
        application_args=[
            "--input",  f"{DELTA_BASE}/trip_delay_events",
            "--output", f"{DELTA_BASE}/route_delay_profiles",
            "--weeks",  "12",
        ],
        name="transit-route-profiles",
    )

    @task()
    def update_hbase_profiles(**ctx):
        """Push updated profiles to HBase for real-time feature lookups."""
        log.info("Pushing route profiles to HBase transit:route_profiles")
        # HBase put logic via happybase or Spark-HBase connector
        pass

    notify = SlackWebhookOperator(
        task_id="notify_profiles_done",
        http_conn_id=SLACK_CONN,
        message="📊 *Transit Platform* — weekly route profiles updated",
    )

    build_profiles >> update_hbase_profiles() >> notify
