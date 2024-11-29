from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime,timedelta
from databrain_utils.initialize import create_initialize_task
from wecom_alerting import make_wecom_alerting
from airflow.models import Variable

webhook_keys = {
    'dev': 'wecom_key_data_dev',
    'master': 'wecom_key_data_pro',
}
branches = {
    'dev': 'dev',
    'master': 'master',
}
dbt_envs = {
    'dev': 'dev',
    'master': 'prod',
}
ssh_conn_ids = {
    'dev': 'databrain_opinion_dev',
    'master': 'databrain_opinion_hok',
}
project = "bigquery_dbt"
project_root = "/data"


def create_dag(env, schedule):
    dag_id = f'dbt_kol_tag_{env}'
    project_name = f'{project}_{env}'
    project_dir = f'{project_root}/{project_name}'
    branch = branches.get(env, "master")
    ssh_conn_id = ssh_conn_ids.get(env, "")
    webhook_key = Variable.get(webhook_keys.get(env))
    conda = 'miniconda3'
    active_conda = f'source ~/{conda}/etc/profile.d/conda.sh && conda activate {project_name} && '
    git_username = Variable.get("databrain_data_git_user")
    git_password = Variable.get("databrain_data_git_password")
    repo_url = f"https://{git_username}:{git_password}@e.coding.intlgame.com/ptc/databrain_data/bigquery_dbt.git"
 
    default_args = {
        'owner': 'clzhu',
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=1),
        'on_failure_callback': make_wecom_alerting(webhook_key, mentioned_list=['clzhu']),
    }

    dag = DAG(
        dag_id,
        default_args=default_args,
        schedule_interval=schedule,  # UTC+0
        start_date=datetime(2024, 6, 20),
        dagrun_timeout=timedelta(hours=2),
        tags=['dbt', project],
        catchup=False,
        max_active_runs=1,
        concurrency=1,
    )

    initialize = create_initialize_task(dag, ssh_conn_id, repo_url, project_name, project_root, branch, conda)

    dbt_env = dbt_envs.get(env, env)
    dbt_task = SSHOperator(
        dag=dag,
        ssh_conn_id=ssh_conn_id,
        task_id='dbt_task',
        cmd_timeout=720,
        command=f'cd {project_dir} && '
                f'{active_conda}'
                f'dbt run --target {dbt_env} --select "streamhatchet_kol_tag generated_kol_partner kol_tag"',
    )

    initialize >> dbt_task

    return dag


dag_dev = create_dag('dev', '15 18 * * *')
dag_master = create_dag('master', '10 18 * * *')
