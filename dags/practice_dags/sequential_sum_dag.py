from airflow.sdk import chain, dag, task
from pendulum import datetime

@dag(
    start_date=datetime(2025, 7, 5),
    schedule=None,  # Manual trigger only
    catchup=False,
    description="🚀 DEPLOYED TO ASTRO CLOUD! A simple DAG with 4 sequential tasks that add numbers 1, 2, 3, 4",
    tags=["practice", "sequential", "sum"]
)
def sequential_sum_dag():
    """
    A simple DAG that demonstrates sequential task execution.
    Each task adds a number (1, 2, 3, 4) to a running sum.
    """
    
    @task
    def add_one() -> int:
        """Add 1 to the sum"""
        result = 0 + 1
        print(f"Step 1: Adding 1 to 0 = {result}")
        return result
    
    @task
    def add_two(previous_sum: int) -> int:
        """Add 2 to the previous sum"""
        result = previous_sum + 2
        print(f"Step 2: Adding 2 to {previous_sum} = {result}")
        return result
    
    @task
    def add_three(previous_sum: int) -> int:
        """Add 3 to the previous sum"""
        result = previous_sum + 3
        print(f"Step 3: Adding 3 to {previous_sum} = {result}")
        return result
    
    @task
    def add_four(previous_sum: int) -> int:
        """Add 4 to the previous sum and show final result"""
        result = previous_sum + 4
        print(f"Step 4: Adding 4 to {previous_sum} = {result}")
        print(f"🎉 Final sum: {result} (1+2+3+4 = 10)")
        return result
    
    # Create task instances
    step1 = add_one()
    step2 = add_two(step1)
    step3 = add_three(step2)
    step4 = add_four(step3)
    
    # Chain tasks together (optional, as they're already chained via dependencies)
    chain(step1, step2, step3, step4)

# Instantiate the DAG
sequential_sum_dag() 