# Part 3: Running & Monitoring DAGs - Alerts & Worker Queues

## 🎯 **Assignment Overview**
This document covers the implementation of Part 3 requirements:
1. ✅ **Run DAGs on Astro Hosted Deployment** - Already running
2. ✅ **Configure tasks on different worker queues** - Implemented in `parallel_math_dag.py`
3. 🚨 **Create meaningful DAG alerts** - Instructions below

---

## 🏗️ **Worker Queue Configuration**

### **Implementation Details**
Modified `parallel_math_dag.py` to demonstrate 3 different worker queues:

| Queue Name | Tasks | Purpose |
|------------|--------|---------|
| `default` | `calculate_sum_1_to_10()`, `summarize_all_results()` | Simple operations |
| `memory_intensive` | `calculate_multiplication_table_5()`, `calculate_squares()` | Memory-heavy operations |
| `cpu_intensive` | `calculate_factorial_series()`, `calculate_fibonacci_sequence()` | CPU-heavy operations |

### **Code Example**
```python
@task(queue="cpu_intensive")
def calculate_factorial_series() -> dict:
    """Calculate factorials of numbers 1 to 6 - CPU INTENSIVE QUEUE"""
    print("⚡ Running on CPU_INTENSIVE worker queue")
    # ... task logic
```

### **Benefits**
- **Resource Optimization**: CPU-intensive tasks run on dedicated workers
- **Scalability**: Different queues can have different scaling policies
- **Cost Efficiency**: Memory-intensive tasks get appropriate resources
- **Monitoring**: Easy to track queue performance separately

---

## 🚨 **Astro Cloud Alert Types & Configuration**

### **Available Alert Types**

#### **1. DAG Alerts 📊**
- **DAG Success**: Trigger when entire DAG run completes successfully
- **DAG Failure**: Trigger when DAG run fails
- **DAG Long Running**: Trigger when DAG exceeds expected duration

#### **2. Task Alerts ⚡**
- **Task Success**: Trigger when specific task completes successfully
- **Task Failure**: Trigger when specific task fails
- **Task Retry**: Trigger when task retries exceed threshold
- **Task SLA Miss**: Trigger when task misses SLA deadline

#### **3. Resource Alerts 🏗️**
- **Worker Queue Length**: Trigger when queue backlog exceeds threshold
- **Worker CPU Usage**: Trigger when CPU usage exceeds limit
- **Worker Memory Usage**: Trigger when memory usage exceeds limit

#### **4. Custom Alerts 🎯**
- **Custom Metrics**: Based on custom metrics from your DAGs
- **Business Logic**: Based on data quality or business rules

---

## 🚨 **Recommended Alert Configuration**

### **For `fetch_data` DAG (GenAI Pipeline)**
```yaml
Alert Type: DAG Failure
Condition: DAG fails
Severity: Critical
Action: Email + Slack notification
Reason: Data pipeline failure impacts downstream ML models
```

### **For `parallel_math_dag` (Worker Queue Demo)**
```yaml
Alert Type: Task Failure
Condition: Any task in cpu_intensive queue fails
Severity: Warning
Action: Email notification
Reason: Monitor worker queue performance
```

### **For General Monitoring**
```yaml
Alert Type: DAG Long Running
Condition: DAG runs longer than 30 minutes
Severity: Warning
Action: Email notification
Reason: Detect performance degradation
```

---

## 📋 **Step-by-Step Alert Setup in Astro Cloud**

### **Step 1: Access Alerts**
1. Login to your Astro Cloud dashboard
2. Navigate to your deployment
3. Click on **"Alerts"** in the left sidebar
4. Click **"Create Alert"**

### **Step 2: Configure Alert**
1. **Select Alert Type**: Choose from DAG/Task/Resource alerts
2. **Set Conditions**: Define when the alert should trigger
3. **Choose Severity**: Critical, Warning, or Info
4. **Configure Actions**: Email, Slack, webhook, or PagerDuty
5. **Set Recipients**: Who should receive notifications

### **Step 3: Test Alert**
1. **Trigger Condition**: Manually trigger the alert condition
2. **Verify Notification**: Check that notifications are received
3. **Adjust Settings**: Fine-tune thresholds and recipients

---

## 🎯 **Meaningful Alert Examples**

### **Alert 1: Critical GenAI Pipeline Failure**
```yaml
Name: "GenAI Pipeline Critical Failure"
Type: DAG Failure
DAG: fetch_data
Condition: DAG fails
Severity: Critical
Recipients: data-team@company.com, on-call-engineer@company.com
Actions: 
  - Email with error details
  - Slack #data-alerts channel
  - PagerDuty incident
Reason: GenAI pipeline failure blocks ML model training
```

### **Alert 2: Worker Queue Performance**
```yaml
Name: "CPU Intensive Queue Backlog"
Type: Worker Queue Length
Queue: cpu_intensive
Condition: Queue length > 10 tasks
Severity: Warning
Recipients: devops-team@company.com
Actions:
  - Email notification
  - Slack #airflow-monitoring
Reason: Indicates need for worker scaling
```

### **Alert 3: SLA Monitoring**
```yaml
Name: "Math Operations SLA Miss"
Type: Task SLA Miss
DAG: parallel_math_dag
Task: summarize_all_results
Condition: Task duration > 5 minutes
Severity: Warning
Recipients: performance-team@company.com
Actions:
  - Email notification
Reason: Monitor task performance degradation
```

---

## 🚀 **Implementation Status**

### ✅ **Completed**
- [x] Worker queue configuration in `parallel_math_dag.py`
- [x] Three different queues: default, memory_intensive, cpu_intensive
- [x] Queue logging and monitoring in task outputs
- [x] DAG descriptions and tags updated

### 🚨 **Next Steps (Manual Setup Required)**
- [ ] Login to Astro Cloud dashboard
- [ ] Navigate to Alerts section
- [ ] Create "GenAI Pipeline Critical Failure" alert
- [ ] Create "Worker Queue Performance" alert
- [ ] Test alerts by triggering failure conditions
- [ ] Document alert notification history

---

## 🔧 **Testing Your Setup**

### **Test Worker Queues**
1. **Run `parallel_math_dag`** in Astro Cloud
2. **Check logs** for queue assignments:
   ```
   🔍 Running on DEFAULT worker queue
   🧠 Running on MEMORY_INTENSIVE worker queue
   ⚡ Running on CPU_INTENSIVE worker queue
   ```
3. **Monitor queue performance** in Astro Cloud UI

### **Test Alerts**
1. **Trigger DAG failure**: Comment out a required import
2. **Verify alert notification**: Check email/Slack
3. **Fix DAG**: Restore normal operation
4. **Document alert response time**

---

## 🎯 **Assignment Completion Checklist**

- [x] **DAGs running on Astro Hosted Deployment**
- [x] **Tasks configured on different worker queues**
- [ ] **Meaningful alert created and tested**
- [ ] **Alert notification received and verified**
- [ ] **Documentation of alert types explored**

---

*This completes Part 3 of the Astronomer GenAI Workflow Orchestration assignment. The implementation demonstrates advanced Airflow concepts including worker queue management and comprehensive monitoring strategies.* 