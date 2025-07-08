from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    'owner': 'sales-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'serial_data_pipeline',
    default_args=default_args,
    description='Serial DAG with 4 sequential tasks',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['serial', 'data-pipeline'],
)

# Task 1: Data Extraction
@task(dag=dag)
def extract_data():
    """Extract data from source system"""
    print("Extracting data from source system...")
    # Simulate data extraction
    data = {
        'customers': 1000,
        'orders': 5000,
        'products': 250
    }
    return data

# Task 2: Data Transformation
@task(dag=dag)
def transform_data(raw_data):
    """Transform extracted data"""
    print(f"Transforming data: {raw_data}")
    # Simulate data transformation
    transformed_data = {
        'total_revenue': raw_data['orders'] * 85.50,
        'avg_order_value': 85.50,
        'customer_count': raw_data['customers']
    }
    return transformed_data

# Task 3: Data Quality Check
@task(dag=dag)
def quality_check(transformed_data):
    """Perform data quality checks"""
    print(f"Quality checking: {transformed_data}")
    # Simulate quality checks
    if transformed_data['total_revenue'] > 0:
        print("✅ Data quality check passed")
        return True
    else:
        raise ValueError("❌ Data quality check failed")

# Task 4: Load to Snowflake
load_to_snowflake = SnowflakeOperator(
    task_id='load_to_snowflake',
    dag=dag,
    snowflake_conn_id='snowflake_default',
    sql="""
    CREATE TABLE IF NOT EXISTS sales_summary (
        date DATE,
        total_revenue DECIMAL(10,2),
        avg_order_value DECIMAL(10,2),
        customer_count INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    INSERT INTO sales_summary (date, total_revenue, avg_order_value, customer_count)
    VALUES ('{{ ds }}', 427500.00, 85.50, 1000);
    """,
)

# Define task dependencies
extracted_data = extract_data()
transformed_data = transform_data(extracted_data)
quality_passed = quality_check(transformed_data)
quality_passed >> load_to_snowflake