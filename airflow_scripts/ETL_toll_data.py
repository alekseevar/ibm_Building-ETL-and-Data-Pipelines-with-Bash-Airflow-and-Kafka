from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'Ramesh Sannareddy',
    'start_date': days_ago(0),
    'email': ['ramesh@somemail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache AirFlow Final Assignment',
    schedule_interval=timedelta(days=1),
)


unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar zxvf finalassignment/tolldata.tgz',
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command="cut -d':' -f1,2,3,4 finalassignment/vehicle-data.csv > finalassignment/csv_data.csv",
    dag=dag,
)


extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="echo \"$(cut -d$'\t' -f4,5 finalassignment/tollplaza-data.tsv)\" | tr '\t' ',' > finalassignment/tsv_data.csv",
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="echo \"$(cut -c59-61,62-67 finalassignment/payment-data.txt)\" | tr ' ' ',' > finalassignment/fixed_width_data.csv",
    dag=dag,
)


consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="paste -d ',' finalassignment/csv_data.csv finalassignment/tsv_data.csv finalassignment/fixed_width_data.csv > finalassignment/extracted_data.csv",
    dag=dag,
)


transform_data = BashOperator(
    task_id='transform_data',
    bash_command="sed -E 's/^(([^,]+,){3})([^,]+)/\1\U\3/' finalassignment/extracted_data.csv  > finalassignment/staging/transformed_data.csv",
    dag=dag,
)


unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
