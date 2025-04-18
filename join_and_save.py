#!/usr/bin/env python
# coding: utf-8

# In[2]:

from pyspark.sql import SparkSession 
spark=SparkSession.builder\
.appName("Test spark")\
.getOrCreate()
print(spark)


# In[3]:


mysql_url = "jdbc:mysql://localhost:3306/cricketers_db"
mysql_properties = {
    "user": "root",
    "password": "root12345",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read MySQL Table into PySpark DataFrame
table_name = "cricketers"
df =spark.read.jdbc(url=mysql_url, table=table_name, properties=mysql_properties)

# Show Data
df.show()


# In[4]:


df_csv = spark.read.csv("D:\\csv file\\cricketers.csv", header=True, inferSchema=True)

# Step 3: Show Data
print("📄 Data from CSV:")
df_csv.show()


# In[3]:


from pyspark.sql import SparkSession 
spark=SparkSession.builder\
.appName("Test spark")\
.getOrCreate()


# In[8]:


df_csv = spark.read.csv("D:\\csv file\\cricketers.csv", header=True, inferSchema=True)


# In[13]:


from pyspark.sql import SparkSession

# Step 1: Create Spark Session
spark = SparkSession.builder \
    .appName("Join MySQL and CSV") \
    .getOrCreate()

# Step 2: Read CSV file
df_csv = spark.read.csv("D:\\csv file\\cricketers.csv", header=True, inferSchema=True)

# Step 3: Read MySQL Table
mysql_url = "jdbc:mysql://localhost:3306/cricketers_db"
mysql_properties = {
    "user": "root",
    "password": "root12345",
    "driver": "com.mysql.cj.jdbc.Driver"
}
table_name = "cricketers"
df_mysql = spark.read.jdbc(url=mysql_url, table=table_name, properties=mysql_properties)

# Step 4: Join on 'id' column (use 'outer' or 'full' if needed)
df_joined = df_mysql.join(df_csv, on="id", how="full")

# Step 5: Show the result
print("✅ Joined DataFrame:")
df_joined.show()



# In[3]:


from pyspark.sql import SparkSession

# Step 1: Create Spark Session
spark = SparkSession.builder \
    .appName("Join MySQL and CSV") \
    .getOrCreate()

# Step 2: Read CSV file
df_csv = spark.read.csv("D:\\csv file\\cricketers.csv", header=True, inferSchema=True)

# Step 3: Read MySQL Table
mysql_url = "jdbc:mysql://localhost:3306/cricketers_db"
mysql_properties = {
    "user": "root",
    "password": "root12345",
    "driver": "com.mysql.cj.jdbc.Driver"
}
table_name = "cricketers"
df_mysql = spark.read.jdbc(url=mysql_url, table=table_name, properties=mysql_properties)

# Step 4: Join on 'id' column (use 'outer' or 'full' if needed)
df_joined = df_mysql.join(df_csv, on="id", how="full")

# Step 5: Show the result
print("✅ Joined DataFrame:")
df_joined.show()



# In[4]:


from pyspark.sql import SparkSession

# Create Spark session with MySQL JDBC driver
spark = SparkSession.builder \
    .appName("SaveToMySQL") \
    .config("spark.jars", "C:\\mysql-connector-j-9.1.0.jar") \
    .getOrCreate()

# Define JDBC connection details
jdbc_url = "jdbc:mysql://localhost:3306/cricketers_db"
table_name = "Cricketers_report"  # This is the destination table name
connection_properties = {
    "user": "root",
    "password": "root12345",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Save the joined DataFrame to MySQL
df_joined.write \
    .jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)

print("✅ DataFrame successfully written to MySQL table 'Cricketers_report'")


# In[5]:


df_pandas = df_joined.toPandas()


# In[6]:


import os

# Ensure the directory exists
os.makedirs("D:/csv file", exist_ok=True)

# Now save the DataFrame
df_pandas.to_csv("D:/csv file/Cricketers_report.csv", index=False, header=True)
print(df_pandas)  # Prints the whole DataFrame


# In[7]:


from pyspark.sql import SparkSession

# Create Spark session with MySQL JDBC driver
spark = SparkSession.builder \
    .appName("JoinMySQLAndCSV") \
    .config("spark.jars", "C:\\mysql-connector-j-9.1.0.jar") \
    .getOrCreate()

# Read CSV
df_csv = spark.read.csv("D:\\csv file\\cricketers.csv", header=True, inferSchema=True)

# Read from MySQL
jdbc_url = "jdbc:mysql://localhost:3306/cricketers_db"
table_name = "cricketers"
connection_properties = {
    "user": "root",
    "password": "root12345",
    "driver": "com.mysql.cj.jdbc.Driver"
}
df_mysql = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

# Join DataFrames
df_joined = df_mysql.join(df_csv, on="id", how="full")

# Save to MySQL
output_table = "Cricketers_report"
df_joined.write.jdbc(url=jdbc_url, table=output_table, mode="overwrite", properties=connection_properties)

print("✅ Successfully joined and saved data to MySQL.")


# In[8]:


from pyspark.sql import SparkSession

# Step 1: Create Spark Session
spark = SparkSession.builder \
    .appName("Join MySQL and CSV") \
    .config("spark.jars", "C:/Users/sarav/Downloads/mysql-connector-j-9.1.0/mysql-connector-j-9.1.0/mysql-connector-j-9.1.0.jar") \
    .getOrCreate()

# Step 2: Read CSV file
df_csv = spark.read.csv("D:/csv file/cricketers.csv", header=True, inferSchema=True)

# Step 3: Read MySQL Table
mysql_url = "jdbc:mysql://localhost:3306/cricketers_db"
mysql_properties = {
    "user": "root",
    "password": "root12345",
    "driver": "com.mysql.cj.jdbc.Driver"
}
table_name = "cricketers"
df_mysql = spark.read.jdbc(url=mysql_url, table=table_name, properties=mysql_properties)

# Step 4: Join on 'id' column
df_joined = df_mysql.join(df_csv, on="id", how="full")

# Step 5: Write to MySQL
df_joined.write.jdbc(url=mysql_url, table="Cricketers_report", mode="overwrite", properties=mysql_properties)


# In[ ]:




