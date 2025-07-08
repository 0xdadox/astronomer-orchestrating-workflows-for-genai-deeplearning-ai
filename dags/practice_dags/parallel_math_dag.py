from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    start_date=datetime(2025, 7, 5),
    schedule=None,  # Manual trigger only
    catchup=False,
    description="A parallel DAG with 4 independent tasks performing different math operations",
    tags=["practice", "parallel", "math"]
)
def parallel_math_dag():
    """
    A DAG that demonstrates parallel task execution.
    All 4 tasks run independently and simultaneously.
    """
    
    @task
    def calculate_sum_1_to_10() -> int:
        """Calculate the sum of numbers 1 to 10"""
        result = sum(range(1, 11))
        print(f"🔢 Sum of 1 to 10: {result}")
        print(f"Calculation: 1+2+3+4+5+6+7+8+9+10 = {result}")
        return result
    
    @task
    def calculate_multiplication_table_5() -> str:
        """Calculate multiplication table of 5 (5x1 to 5x10)"""
        results = []
        for i in range(1, 11):
            product = 5 * i
            results.append(f"5 × {i} = {product}")
        
        result_str = "\n".join(results)
        print(f"🔢 Multiplication table of 5:")
        print(result_str)
        return result_str
    
    @task
    def calculate_squares() -> list:
        """Calculate squares of numbers 1 to 8"""
        squares = []
        for i in range(1, 9):
            square = i ** 2
            squares.append(square)
            print(f"{i}² = {square}")
        
        print(f"🔢 Squares of 1 to 8: {squares}")
        return squares
    
    @task
    def calculate_factorial_series() -> dict:
        """Calculate factorials of numbers 1 to 6"""
        factorials = {}
        
        def factorial(n):
            if n <= 1:
                return 1
            return n * factorial(n - 1)
        
        for i in range(1, 7):
            fact = factorial(i)
            factorials[str(i)] = fact  # Convert key to string for XCom compatibility
            print(f"{i}! = {fact}")
        
        print(f"🔢 Factorials 1! to 6!: {factorials}")
        return factorials
    
    # Create task instances - these will run in parallel
    sum_task = calculate_sum_1_to_10()
    multiplication_task = calculate_multiplication_table_5()
    squares_task = calculate_squares()
    factorial_task = calculate_factorial_series()
    
    # No dependencies = parallel execution!
    # All tasks will start at the same time

# Instantiate the DAG
parallel_math_dag() 