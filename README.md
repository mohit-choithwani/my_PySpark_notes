# Apache Spark

Apache Spark is a powerful distributed computing framework that provides an easy-to-use interface for data processing, analytics, and machine learning. It is built on top of the Hadoop ecosystem and is designed to be fast, flexible, and easy to use.

Here are some scenarios where Apache Spark might be a good choice:
1. If you are dealing with large amout of data which doesn't fit into the meomory of a single machine. Spark can distribute the data across a cluster of machines and process it in parallel.

2. Spark is highly scalable. You can increase the number of machines in the cluster for computing (Horizontal Scaling).

3. Spark is fast because of its in-memory computing capabilities. It can cache the data in memory and reuse it for multiple computations.

In the following, we will explore some feature of Apache spark in python and see how we can use it for data processing and machine learning.

### Spark Architecture
Spark follows a master-slave architecture:

 - Driver (Master): Manages application execution.
 - Executors (Slaves): Perform assigned tasks on worker nodes.
 - Cluster Manager: Assigns resources and schedules tasks.

The cluster manager, which is responsible for managing/assigning the resources of the cluster and scheduling the tasks on the executors. 

![Spark Architecture](assets/spark-architecture.png)    

### Getting Started
I have create a notebook inside databricks to perform some operations on spark. (I have created a free account on databricks)
I have used a dataset from kaggle () download the dataset and load it into databricks.

### Data Reading
Load the dataset into a PySpark DataFrame:
```python
data = spark.read.format('csv').option('inferSchema', True).option('header', True).load('/FileStore/tables Beach_Weather_Stations___Automated_Sensors.csv')
```

### Data Display
```python
data.display()
```

### Data Analytics and SQL operations
Once the data is stored in a dataframe, we can use sql function to perform operations. We need to import this library from pyspark.sql.functions import *

#### 1. Selecting columns
```python
data.select(col('Station Name', 'Humidity')).display()  
```
Note:- We can also directly use the columns names but a few functions such as alias only works with col() function.

#### 2. Alias
```python
data.select(col('Station Name').alias('Data Source')).display()
```

#### 3. Filter/Where
```python   
data.filter(col('Station Name') == 'Foster Weather Station').display()
```
We can also use the multiple conditions in the filter function.
'''python
data.filter(
    (col('Station Name') != 'Foster Weather Station') & \
    (col('Humidity') > 20) \
    ).display()
```

#### 4. Column Rename
```python
data.withColumnRenamed('Station Name', 'Data Source').display()
```

#### 5. withColumn 
Used for creating new columns or modifying existing columns.
```python   
data.withColumn('Station Name', regexp_replace(col('Station Name'), 'Foster Weather Station', 'Station 2')).display()   
``` 

#### 6. Sort/Order By
```python   
data.sort(col('Humidity').desc()).display()  
```

'''python
data.sort([col('Air Temperature'), col('Humidity')], ascending=[1, 0]).display()
'''

#### 7. Limit
```python
data.limit(100).display()
``` 

#### 8. Drop Columns
```python   
data.drop('Station Name').display()
```

#### 9. Drop Duplicates
```python
data.dropDuplicates().display()
```

#### 10. Missing Values
Two options - a. Remove rows which contain null values (dropping nulls) <br>
b. Fill the null values with mean or some other value (filling nulls)
```python
df.dropna(subset=['Wet Bulb Temperature']).display()    

df.fillna(0, subset=['Wet Bulb Temperature']).display()
```     

#### 11. Group By
```python
df.groupBy('Station Name').agg(sum('Air Temperature')).display()
```
'''python
# Groupby on multiple columns
df.groupby('Station Name').agg(max('Air Temperature').alias('Max Temp'), avg('Air Temperature').alias('Avg Temp')).display()
'''

#### 12. When - Otherwise
```python
# Create a new column named "Temperature" which displays high, low depending on the Air Temperature value
df.withColumn('Temperature', when(col('Air Temperature')>20, 'high').otherwise('low')).display()
'''

#### 13. Join
I have solved multiple join problems in the notebook. Please refer to the notebook for the solution.
basic syntax:- 
```python
df1.join(df2, df1['column_name'] == df2['column_name'], 'inner').display()
```

#### 14. Window Functions
You need to import the window function from pyspark.sql.window import Window
```python
from pyspark.sql.window import Window
```

Example:- 
```python
users_df.withColumn('row_number', \
    row_number().over(Window.orderBy('city'))\
    ).display()
```

'''python
users_df.withColumn('row rank', rank().over(Window.orderBy(col('age').desc())))\
    .withColumn('row dense rank', dense_rank().over(Window.orderBy(col('age').desc()))).display()
'''

#### 15. UDF
User Defined Functions (UDF) are used to create custom functions in PySpark. 
```python
from pyspark.sql.functions import udf

# Step 1: Define a Python function
def square(x):
    return x * x

# Step 2: Register as UDF
square_udf = udf(square)

# Step 3: Use the UDF
data.withColumn('Square Value', square_udf('Column Name')).display()

'''

