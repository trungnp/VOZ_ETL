import json
from datetime import datetime

import pendulum
from airflow.decorators import task, dag, task_group
from airflow.models import Variable

# noinspection PyUnresolvedReferences
from extract.extract_pg import extract_threads_pg, extract_users_pg
# noinspection PyUnresolvedReferences
from transform.transform import transform_participants, transform_threads, transform_users
# noinspection PyUnresolvedReferences
from load.load_s3_to_redshift import load_s3_to_redshift, load_stage_to_prod_redshift
# noinspection PyUnresolvedReferences
from utilities.database_utils import connect

S3_SECKET_KEY = Variable.get("s3_secret")
S3_ACCESS_KEY = Variable.get("s3_access")
S3_BUCKET = Variable.get("s3_bucket")

pg_engine = connect(section="postgresql_aws")

redshift_params = Variable.get("redshift", deserialize_json=True)
iam = redshift_params['iam']
rs_conn = connect(section="redshift")

default_args = {'start_date': pendulum.today("UTC").add(days=-1), 'owner': 'vozlit', 'depends_on_past': False}


@dag(dag_id='ETL_voz', schedule_interval=None, default_args=default_args, catchup=False)
def ETL():
    latest_update_date = pg_engine.execute("SELECT * FROM latest_update_date").fetchone()
    extraction_date = datetime.now().strftime("%Y%m%d%H%M%S")

    @task_group(group_id='extract')
    def extract():
        @task(task_id='ext_threads', multiple_outputs=True)
        def extract_threads():

            return extract_threads_pg(pg_engine, S3_BUCKET=S3_BUCKET, S3_ACCESS_KEY=S3_ACCESS_KEY, S3_SECKET_KEY=S3_SECKET_KEY)

        @task(task_id='ext_users')
        def extract_users():
            return extract_users_pg(pg_engine, S3_BUCKET=S3_BUCKET, S3_ACCESS_KEY=S3_ACCESS_KEY, S3_SECKET_KEY=S3_SECKET_KEY)

        @task(task_id='ext_participants')
        def extract_participants(fp):
            return fp['participants']

        threads = extract_threads()
        users = extract_users()
        participants = extract_participants(threads)

        paths = {'threads': threads['threads'],
                 'users': users,
                 'participants': participants}

        return paths


    @task_group(group_id='transform')
    def transform(data):
        @task(task_id='tf_users')
        def transform_u(fp):
            return transform_users(S3_BUCKET=S3_BUCKET, S3_ACCESS_KEY=S3_ACCESS_KEY, S3_SECKET_KEY=S3_SECKET_KEY, fp=fp)

        @task(task_id='tf_threads')
        def transform_t(fp):
            return transform_threads(S3_BUCKET=S3_BUCKET, S3_ACCESS_KEY=S3_ACCESS_KEY, S3_SECKET_KEY=S3_SECKET_KEY, fp=fp)

        @task(task_id='tf_participants')
        def transform_p(fp):
            return transform_participants(S3_BUCKET=S3_BUCKET, S3_ACCESS_KEY=S3_ACCESS_KEY, S3_SECKET_KEY=S3_SECKET_KEY, fp=fp)

        threads = transform_t(data['threads'])
        users = transform_u(data['users'])
        participants = transform_p(data['participants'])

        return {'threads': threads,
                'users': users,
                'participants': participants}

    @task_group(group_id='load')
    def load(data):

        @task_group(group_id='s3_to_stage_rs')
        def load_s3_to_redshift_stage(data):
            @task(task_id='s3_stage_threads')
            def load_threads_s3_stage(fp, table):
                file = f's3://{S3_BUCKET}/{fp}'
                load_s3_to_redshift(rs_conn, file, iam, table)

                return table

            @task(task_id='s3_stage_users')
            def load_users_s3_stage(fb, table):
                file = f's3://{S3_BUCKET}/{fb}'
                load_s3_to_redshift(rs_conn, file, iam, table)

                return table

            t = load_threads_s3_stage(data['threads'], 'threads_staging')
            u = load_users_s3_stage(data['users'], 'users_staging')

            return {'threads': t, 'users': u, 'participants': data['participants']}

        @task_group(group_id='stage_to_prod_rs')
        def load_redshift_stage_to_production(stage):
            @task(task_id='threads_stage_production')
            def load_threads_stage_prod(stg, prod):
                load_stage_to_prod_redshift(rs_conn, stg, prod)

                return "success_threads"

            @task(task_id='users_stage_production')
            def load_users_stage_prod(stg, prod):
                load_stage_to_prod_redshift(rs_conn, stg, prod)

                return "success_users"

            @task(task_id='s3_production_participants')
            def load_participants_s3_prod(fb, table):
                file = f's3://{S3_BUCKET}/{fb}'
                load_s3_to_redshift(rs_conn, file, iam, table)

                return "success_participants"

            t = load_threads_stage_prod(stage['threads'], 'threads')
            u = load_users_stage_prod(stage['users'], 'users')
            p = load_participants_s3_prod(stage['participants'], 'participants')

            return {'threads': t, 'users': u, 'participants': p, 'update': 'success'}

        s3_stage = load_s3_to_redshift_stage(data)
        stage_prod = load_redshift_stage_to_production(s3_stage)

        return stage_prod

    @task(task_id='upd_latest_update_date')
    def update_date(ld_):
        new_latest_update = "SELECT MAX(updated_at) FROM threads"
        pg_engine.execute(
                f"TRUNCATE latest_update_date; INSERT INTO latest_update_date ({new_latest_update});")

        return f"{ld_['update']}_update"

    upd = update_date(load(transform(extract())))

    return upd


dag = ETL()
