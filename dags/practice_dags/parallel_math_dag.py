from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    start_date=datetime(2025, 7, 8),
    schedule=None,  # Manual trigger only
    catchup=False,
    description="🎯 ASTRO CLOUD DEPLOYED! Enhanced parallel DAG with 5 independent math operations + summary",
    tags=["practice", "parallel", "math", "deployed", "enhanced"]
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
    
    @task
    def calculate_fibonacci_sequence() -> list:
        """Calculate Fibonacci sequence up to 12 numbers - NEW TASK for Astro Cloud!"""
        fibonacci = [0, 1]
        
        for i in range(2, 12):
            next_fib = fibonacci[i-1] + fibonacci[i-2]
            fibonacci.append(next_fib)
            print(f"F({i}) = {next_fib}")
        
        print(f"🌟 NEW! Fibonacci sequence (12 numbers): {fibonacci}")
        return fibonacci
    
    @task
    def summarize_all_results(sum_result: int, multiplication_result: str, 
                             squares_result: list, factorial_result: dict, 
                             fibonacci_result: list) -> dict:
        """🎯 ASTRO CLOUD FEATURE: Summarize all parallel math operations"""
        print("=" * 60)
        print("🎯 ASTRO CLOUD DEPLOYMENT - MATH OPERATIONS SUMMARY")
        print("=" * 60)
        print(f"📊 Sum 1-10: {sum_result}")
        print(f"✖️  Multiplication table 5: {len(multiplication_result.split('\\n'))} operations")
        print(f"2️⃣  Squares calculated: {len(squares_result)} numbers")
        print(f"❗ Factorials calculated: {len(factorial_result)} numbers")
        print(f"🌟 NEW! Fibonacci sequence: {len(fibonacci_result)} numbers")
        print("=" * 60)
        print("🚀 All operations completed successfully in Astro Cloud!")
        print("=" * 60)
        
        return {
            "total_operations": 5,
            "deployment_status": "SUCCESS",
            "astro_cloud_deploy": True,
            "results_summary": {
                "sum_result": sum_result,
                "squares_count": len(squares_result),
                "factorials_count": len(factorial_result),
                "fibonacci_count": len(fibonacci_result)
            }
        }

    # Create task instances - first 5 run in parallel
    sum_task = calculate_sum_1_to_10()
    multiplication_task = calculate_multiplication_table_5()
    squares_task = calculate_squares()
    factorial_task = calculate_factorial_series()
    fibonacci_task = calculate_fibonacci_sequence()  # NEW TASK!
    
    # Summary task depends on all parallel tasks
    summary_task = summarize_all_results(
        sum_task, multiplication_task, squares_task, 
        factorial_task, fibonacci_task
    )
    
    # Dependencies: All parallel tasks → Summary task
    [sum_task, multiplication_task, squares_task, factorial_task, fibonacci_task] >> summary_task

# Instantiate the DAG
parallel_math_dag() 