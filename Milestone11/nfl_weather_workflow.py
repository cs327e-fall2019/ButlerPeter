import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 12, 04)
}

staging_dataset = 'nfl_weather_workflow_staging'
modeled_dataset = 'nfl_weather_workflow_modeled'

bq_query_start = 'bq query --use_legacy_sql=false '

create_nfl_stadiums_sql = 'create or replace table ' + modeled_dataset + '''.nfl_stadiums as
                    select *
                    from ''' + staging_dataset + '''.nfl_stadiums''' 

create_nfl_teams_sql = 'create or replace table ' + modeled_dataset + '''.nfl_teams as
                    select *
                    from ''' + staging_dataset + '''.nfl_teams'''

create_spreadspoke_scores_sql = 'create or replace table ' + modeled_dataset + '''.spreadspoke_scores as
                    select *
                    from ''' + staging_dataset + '''.spreadspoke_scores'''


with models.DAG(
        'nfl_weather_workflow',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    create_staging_dataset = BashOperator(
            task_id='create_staging_dataset',
            bash_command='bq --location=US mk --dataset ' + staging_dataset)
    
    create_modeled_dataset = BashOperator(
            task_id='create_modeled_dataset',
            bash_command='bq --location=US mk --dataset ' + modeled_dataset)
    
    load_nfl_stadiums = BashOperator(
            task_id='load_nfl_stadiums',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.nfl_stadiums \
                         "gs://butlerpeter_bucket/dataset2/nfl_stadiums.csv"',
            trigger_rule='one_success')
    
    load_nfl_teams = BashOperator(
            task_id='load_nfl_teams',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.nfl_teams \
                         "gs://butlerpeter_bucket/dataset2/nfl_teams.csv"',
            trigger_rule='one_success')
    
    load_spreadspoke_scores = BashOperator(
            task_id='load_spreadspoke_scores',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.spreadspoke_scores \
                         "gs://butlerpeter_bucket/dataset2/dataset2_spreadspoke_scores.csv"', 
            trigger_rule='one_success')
   
    split = DummyOperator(
            task_id='split',
            trigger_rule='all_done')

    create_nfl_stadiums = BashOperator(
            task_id='create_nfl_stadiums',
            bash_command=bq_query_start + "'" + create_nfl_stadiums_sql + "'", 
            trigger_rule='one_success')
    
    create_nfl_teams = BashOperator(
            task_id='create_nfl_teams',
            bash_command=bq_query_start + "'" + create_nfl_teams_sql + "'", 
            trigger_rule='one_success')
    
    create_spreadspoke_scores = BashOperator(
            task_id='create_spreadspoke_scores',
            bash_command=bq_query_start + "'" + create_spreadspoke_scores_sql + "'", 
            trigger_rule='one_success')
    
    nfl_stadiums_beam = BashOperator(
            task_id='nfl_stadiums_beam',
            bash_command='python /home/jupyter/airflow/dags/nfl_stadiums_single.py')
    
    nfl_stadiums_dataflow = BashOperator(
            task_id='nfl_stadiums_dataflow',
            bash_command='python /home/jupyter/airflow/dags/nfl_stadiums_cluster.py')
        
        
    create_staging_dataset >> create_modeled_dataset >> split
    split >> load_nfl_stadiums >> create_nfl_stadiums >> nfl_stadiums_beam >> nfl_stadiums_dataflow
    split >> load_nfl_teams >> create_nfl_teams
    split >> load_spreadspoke_scores >> create_spreadspoke_scores 