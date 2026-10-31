"""Exercise actual Airflow task failure propagation in an isolated local database."""

import json
import os
import sys
from pathlib import Path


def main():
    if sys.version_info[:2] != (3, 11):
        raise RuntimeError('Python 3.11 required')
    home = Path(os.environ['AIRFLOW_HOME']).resolve()
    database = home / 'airflow.db'
    if (not home.name.startswith('isolated-') or database.exists()
            or os.environ.get('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN') != 'sqlite:///' + str(database)
            or os.environ.get('AIRFLOW__CORE__EXECUTOR') != 'SequentialExecutor'):
        raise RuntimeError('A fresh isolated-* Airflow home and matching SQLite database are required')
    from airflow import DAG
    from airflow.exceptions import AirflowException
    from airflow.models import DagBag
    from airflow.operators.python import PythonOperator
    from airflow.utils import db
    import pendulum

    # Caller supplies a disposable AIRFLOW_HOME/database, no credentials or network.
    db.initdb()
    root = Path(__file__).resolve().parents[2]
    bag = DagBag(str(root / 'airflow/dags/cocoa_daily.py'), include_examples=False)
    if bag.import_errors:
        raise RuntimeError('Application DAG import failed')
    application = bag.get_dag('cocoa_daily')
    expected = {'capture_baseline', 'run_bq_pipeline', 'summary'}
    expected.update('trigger_' + feed for feed in ('cocoa', 'oil', 'weather'))
    expected.update('wait_' + feed + '_rows' for feed in ('cocoa', 'oil', 'weather'))
    assert set(application.task_ids) == expected and application.max_active_runs == 1

    def unavailable():
        raise AirflowException('Intentional isolated source failure')

    def must_not_publish():
        raise AssertionError('Downstream publication must never execute')

    with DAG('isolated_failure_semantics', start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
             schedule=None, catchup=False) as probe:
        source = PythonOperator(task_id='source', python_callable=unavailable, retries=0)
        publish = PythonOperator(task_id='publish', python_callable=must_not_publish, retries=0)
        source >> publish
    run = probe.test(execution_date=pendulum.datetime(2026, 1, 2, tz='UTC'))
    states = {task.task_id: task.state for task in run.get_task_instances()}
    assert run.state == 'failed' and states == {'source': 'failed', 'publish': 'upstream_failed'}, states
    print('RUNTIME_JSON=' + json.dumps({'python': sys.version.split()[0], 'application_tasks': len(application.tasks),
                                      'failure_states': states, 'cloud_io': False}))


if __name__ == '__main__':
    main()
