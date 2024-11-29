from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime,timedelta
from databrain_utils.initialize import create_initialize_task
from opinion_media_account.utils import env_fullname, get_project_info, get_default_args


def create_dag(name, env, schedule_interval=None):
    dag_id = f'opinion_{name}_{env}'
    (project, project_root, project_name, project_dir, branch,
     ssh_conn_id, repo_url, conda, active_conda) = get_project_info(env)
    start_cmd = f'python -m {name} --env {env_fullname.get(env)} --start_time "{{{{params.start_time}}}}" --end_time "{{{{params.end_time}}}}"'

    dag = DAG(
        dag_id,
        default_args=get_default_args(env),
        start_date=datetime(2023, 7, 31),
        dagrun_timeout=timedelta(hours=12),
        tags=['opinion', 'media_account'],
        catchup=False,  # 使用backfill
        max_active_runs=1,  # 一个dag同时运行的最大实例数
        params={'start_time': 'default', 'end_time': 'default'},
        schedule_interval=schedule_interval,  # UTC+0
    )

    initialize = create_initialize_task(dag, ssh_conn_id, repo_url, project_name, project_root, branch, conda)

    run_script = SSHOperator(
        dag=dag,
        ssh_conn_id=ssh_conn_id,
        task_id='run_script',
        cmd_timeout=3600,
        command=f'cd {project_dir}/src && '
                f'{active_conda}'
                f'{start_cmd}',
    )

    initialize >> run_script

    return dag


name = 'kol_tweet_feeds_hourly'
schedule_interval = '5 * * * *'
dag_dev = create_dag(name, 'dev', schedule_interval)
dag_pro = create_dag(name, 'pro', schedule_interval)
