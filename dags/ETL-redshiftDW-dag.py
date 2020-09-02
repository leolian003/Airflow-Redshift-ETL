from datetime import datetime, timedelta
import os
from airflow import DAG
import sys
sys.path.insert(1,'/Users/zheming/airflow')
from airflow.operators.dummy_operator import DummyOperator
from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.load_fact import LoadFactOperator
from plugins.operators.load_dimension import LoadDimensionOperator
from plugins.operators.data_quality import DataQualityOperator
#from airflow.operators.zlPlugin import *
#from airflow.operators.LoadFactOperator import *
#from airflow.operators.LoadDimensionOperator import *
#from airflow.operators.DataQualityOperator import *
from plugins.helpers.sql_queries import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

aws_credentials_id = 'aws_credentials'
redshift_conn_id = 'redshift'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('ETL-redshiftDW',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    aws_credentials_id = aws_credentials_id,
    redshift_conn_id = redshift_conn_id,
    source_table ="staging_events",
    source_format ='json',
    region ="us-west-2",
    s3_key ="log_data",
    s3_bucket ="udacity-dend",
    backfill_date = datetime.utcnow(),
    dag=dag,
    provide_context = True
) 

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    aws_credentials_id = aws_credentials_id,
    redshift_conn_id = redshift_conn_id,
    source_table ="staging_songs",
    source_format ='json',
    region ="us-west-2",
    s3_key ="song_data",
    s3_bucket ="udacity-dend",
    backfill_date = datetime.utcnow(),
    dag=dag,
    provide_context = True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id = redshift_conn_id,
    sql_query = SqlQueries.songplay_table_insert,
    table_name = 'songplays',
    dag=dag,
    provide_context = True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id = redshift_conn_id,
    sql_query = SqlQueries.user_table_insert,
    dag=dag,
    provide_context = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id = redshift_conn_id,
    sql_query = SqlQueries.song_table_insert,
    dag=dag,
    provide_context = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id = redshift_conn_id,
    sql_query = SqlQueries.artist_table_insert,
    dag=dag,
    provide_context = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id = redshift_conn_id,
    sql_query = SqlQueries.time_table_insert,
    dag=dag,
    provide_context = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id = redshift_conn_id,
    tables = ["songplays", "users", "song", "artist", "time"],
    dag=dag,
    provide_context = True
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> (stage_events_to_redshift,stage_songs_to_redshift)>>load_songplays_table>>(load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table)>>run_quality_checks>>end_operator


