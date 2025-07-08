from airflow.sdk import dag, task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime

@dag(
    start_date=datetime(2025, 7, 8),
    schedule=None,
    catchup=False,
    description='Test Snowflake connection and create sample data',
    tags=['test', 'snowflake'],
)
def test_snowflake_connection():
    """
    Test Snowflake connection and create sample data
    """
    
    @task
    def test_connection():
        """Test Snowflake connection using hook"""
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        # Test connection
        records = hook.get_records("SELECT CURRENT_VERSION() as version")
        print(f"Connected to Snowflake version: {records[0][0]}")
        
        return {"connection_test": "passed"}

    @task
    def create_sample_tables():
        """Create sample tables and insert data"""
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        sql_commands = """
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
        """
        
        hook.run(sql_commands)
        print("✅ Sample tables and data created successfully!")
        return {"tables_created": True}

    # Create task instances and dependencies
    connection_test = test_connection()
    tables_task = create_sample_tables()
    
    connection_test >> tables_task

# Instantiate the DAG
test_snowflake_connection()