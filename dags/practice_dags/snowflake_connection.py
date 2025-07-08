from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task

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
    'test_snowflake_connection',
    default_args=default_args,
    description='Test Snowflake connection and create sample data',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'snowflake'],
)

@task(dag=dag)
def test_connection():
    """Test Snowflake connection using hook"""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Test connection
    records = hook.get_records("SELECT CURRENT_VERSION() as version")
    print(f"Connected to Snowflake version: {records[0][0]}")
    
    return {"connection_test": "passed"}

# Create sample tables
create_tables = SnowflakeOperator(
    task_id='create_sample_tables',
    dag=dag,
    snowflake_conn_id='snowflake_default',
    sql="""
    -- Create sample schema
    CREATE SCHEMA IF NOT EXISTS airflow_demo;
    USE SCHEMA airflow_demo;
    
    -- Create customers table
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INTEGER,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        email VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create orders table
    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER,
        customer_id INTEGER,
        order_date DATE,
        total_amount DECIMAL(10,2),
        status VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Insert sample data
    INSERT INTO customers (customer_id, first_name, last_name, email)
    VALUES 
        (1, 'John', 'Doe', 'john.doe@email.com'),
        (2, 'Jane', 'Smith', 'jane.smith@email.com'),
        (3, 'Bob', 'Johnson', 'bob.johnson@email.com');
    
    INSERT INTO orders (order_id, customer_id, order_date, total_amount, status)
    VALUES 
        (1001, 1, '2024-01-15', 299.99, 'completed'),
        (1002, 2, '2024-01-16', 149.50, 'completed'),
        (1003, 1, '2024-01-17', 89.99, 'pending');
    """,
)

connection_test = test_connection()
connection_test >> create_tables