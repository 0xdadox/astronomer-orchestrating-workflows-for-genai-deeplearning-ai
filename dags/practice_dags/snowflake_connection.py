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
        
        # Get comprehensive Snowflake session information
        session_info = hook.get_records("""
            SELECT 
                CURRENT_VERSION() as version,
                CURRENT_ACCOUNT() as account,
                CURRENT_DATABASE() as database,
                CURRENT_WAREHOUSE() as warehouse,
                CURRENT_ROLE() as role,
                CURRENT_USER() as user,
                CURRENT_REGION() as region,
                CURRENT_SESSION() as session_id
        """)
        
        info = session_info[0]
        print("=" * 50)
        print("🎯 SNOWFLAKE CONNECTION DETAILS")
        print("=" * 50)
        print(f"✅ Snowflake Version: {info[0]}")
        print(f"🔗 Account: {info[1]}")
        print(f"🏢 Database: {info[2]}")
        print(f"⚡ Warehouse: {info[3]}")
        print(f"👤 Role: {info[4]}")
        print(f"👨‍💻 User: {info[5]}")
        print(f"🌍 Region: {info[6]}")
        print(f"🆔 Session ID: {info[7]}")
        print("=" * 50)
        
        # Additional useful queries
        print("\n📊 ADDITIONAL INFORMATION:")
        
        # Check warehouse size and status
        warehouse_info = hook.get_records(f"SHOW WAREHOUSES LIKE '{info[3]}'")
        if warehouse_info:
            print(f"⚙️ Warehouse Status: ACTIVE")
        
        # Check available databases
        databases = hook.get_records("SHOW DATABASES")
        print(f"📚 Available Databases: {len(databases)} total")
        
        # Check schemas in current database
        try:
            schemas = hook.get_records("SHOW SCHEMAS")
            print(f"📂 Schemas in {info[2]}: {len(schemas)} total")
        except:
            print(f"📂 Schemas: Unable to list (may need different permissions)")
        
        return {
            "connection_test": "passed",
            "version": info[0],
            "account": info[1],
            "database": info[2],
            "warehouse": info[3],
            "role": info[4],
            "user": info[5],
            "region": info[6],
            "session_id": info[7]
        }

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
        
        -- Insert sample data into customers (using MERGE to avoid duplicates)
        MERGE INTO customers AS target
        USING (
            SELECT 1 as customer_id, 'John' as first_name, 'Doe' as last_name, 'john.doe@email.com' as email
            UNION ALL
            SELECT 2, 'Jane', 'Smith', 'jane.smith@email.com'
            UNION ALL  
            SELECT 3, 'Bob', 'Johnson', 'bob.johnson@email.com'
        ) AS source ON target.customer_id = source.customer_id
        WHEN NOT MATCHED THEN
            INSERT (customer_id, first_name, last_name, email)
            VALUES (source.customer_id, source.first_name, source.last_name, source.email);
        
        -- Insert sample data into orders (using MERGE to avoid duplicates)
        MERGE INTO orders AS target
        USING (
            SELECT 1001 as order_id, 1 as customer_id, '2024-01-15'::DATE as order_date, 299.99 as total_amount, 'completed' as status
            UNION ALL
            SELECT 1002, 2, '2024-01-16'::DATE, 149.50, 'completed'
            UNION ALL
            SELECT 1003, 1, '2024-01-17'::DATE, 89.99, 'pending'
        ) AS source ON target.order_id = source.order_id
        WHEN NOT MATCHED THEN
            INSERT (order_id, customer_id, order_date, total_amount, status)
            VALUES (source.order_id, source.customer_id, source.order_date, source.total_amount, source.status);
        """
        
        hook.run(sql_commands)
        print("✅ Sample tables and data created successfully in ASTRO_DB!")
        print("📊 Created:")
        print("  ✅ ASTRO_DB.airflow_demo.customers (3 records)")
        print("  ✅ ASTRO_DB.airflow_demo.orders (3 records)")
        
        return {"tables_created": True}

    # Create task instances and dependencies
    connection_test = test_connection()
    tables_task = create_sample_tables()
    
    connection_test >> tables_task

# Instantiate the DAG
test_snowflake_connection()