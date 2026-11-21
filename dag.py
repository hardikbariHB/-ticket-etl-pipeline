"""Airflow 2.x DAG that runs the daily ETL. Copy this and etl_pipeline.py into your Airflow dags/ folder.
Not run in this kit (Airflow isn't installed here) -- install Airflow locally or use Docker to test it."""
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import etl_pipeline as etl

def alert_on_failure(context):
    # Replace with email/Slack. Printing is enough to start.
    print(f"ALERT: task {context['task_instance'].task_id} failed for {context['ds']}")

@dag(dag_id="daily_ticket_etl", schedule="@daily", start_date=datetime(2025, 1, 1), catchup=False,
     default_args={"retries": 2, "retry_delay": timedelta(minutes=5), "on_failure_callback": alert_on_failure},
     tags=["portfolio", "etl"])
def daily_ticket_etl():
    @task
    def extract(ds=None):
        return etl.extract(ds)

    @task
    def validate(rows):
        good, rejects = etl.validate(rows)
        return {"good": good, "rejects": rejects, "rows_in": len(rows)}

    @task
    def load(result, ds=None):
        etl.load(result["good"], ds)
        return result

    @task
    def report(result, ds=None):
        status, pct = etl.report(result["rows_in"], result["good"], result["rejects"], ds)
        if status == "FAIL":
            raise ValueError(f"Reject rate {pct}% above threshold on {ds}")

    report(load(validate(extract())))

daily_ticket_etl()
