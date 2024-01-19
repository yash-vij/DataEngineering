from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[1]').appName("SQLConnection")\
    .config("spark.jars.packages","mysql:mysql-connector-java:8.0.13").getOrCreate()
table_list = ['employees','salaries','departments','dept_emp','dept_manager','titles']
df_list = {}
df_and_Columns = {}
for table in table_list:
    df = spark.read \
        .format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/employees") \
        .option("dbtable",table) \
        .option("user", "root") \
        .option("password", "0000") \
        .load()
    df_list[table] = df
    df_and_Columns[table] = df.columns

print(df_list)
for df in df_list.values():
    df.show()

df_list['salaries'].show()


df_new_salary = df_list['salaries']\
    .withColumn("From_Month",date_format(df_list['salaries'].from_date,"MMM"))\
    .withColumn("To_Month",date_format(df_list['salaries'].to_date,"MMM"))\
    .withColumn("From_Year",date_format(df_list['salaries'].from_date,"yyyy"))\
    .withColumn("To_Year",date_format(df_list['salaries'].to_date,"yyyy"))


df_Total_salary = df_new_salary.groupBy('emp_no')\
    .agg(sum('salary').alias('Total_Salary')).orderBy('emp_no')

df_new_employee = df_list['employees'].join(df_Total_salary, \
                                            df_list['employees'].emp_no == df_Total_salary.emp_no, 'left')\
                                                .join(df_list['dept_emp'],df_list['employees'].emp_no ==  df_list['dept_emp'].emp_no,'left')\
                                                .join(df_list['departments'],df_list['dept_emp'].dept_no ==  df_list['departments'].dept_no,'left')\
                                                .select(df_list['employees'].emp_no,df_list['employees'].first_name,df_list['employees'].last_name,df_Total_salary['Total_Salary'],df_list['departments'].dept_name)

df_new_employee.write \
  .format("jdbc") \
  .option("driver","com.mysql.cj.jdbc.Driver") \
  .option("url", "jdbc:mysql://localhost:3306/employees") \
  .option("dbtable", "Employees_pyspark_Transform") \
  .option("user", "root") \
  .option("password", "0000") \
  .mode("overwrite")\
  .save()


df_new_employee.write.mode("overwrite").json("employee_data.json")







