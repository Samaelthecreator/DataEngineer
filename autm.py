from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime,timedelta

default_args = {
        'owner' : 'SITEC',
        'retries' : 5,
        'retry_delay' : timedelta(minutes=2)
    
}


dag = DAG(
    dag_id = 'etl_SITEC',       #Identificador del DAG
    description = 'Extraemos las tablas; compras, ordenes de compra, cotizaciones y rfqs',     #Descripción de lo que realizará
    default_args = default_args,
    start_date = datetime( 2024 , 09 , 09 ),
    schedule_interval = '*/30 8-18 * * 1-5',      #Cron expresion
    #cron expresion: minuto hora dia-mes mes dia-semana
)

#Definimos los task

#EXTRACCIÓN
extract = BashOperator(
    task_id = 'extracción',
    bash_command = "python extract.py"    
)

# transform = BashOperator(
#     task_id = 'transformación',
#     operator_params = '',
#     dag = dag,
# )

# load = BashOperator(
#     task_id = 'carga',
#     operator_params = '',
#     dag = dag,
# )

# #Definimos las relaciones entre las tareas.

extract #<< transform
# transform << load



