# spark
TASK:

# Create a dataframe from cpulogdata csv file using pyspark in and perform the following operations and also visualise it.

# Columns to consider user_name,boot_time,keyboard,mouse 1) Display users and their record counts
2) Finding users with the highest number of average hours.
3) Finding users with the lowest number of average hours.
4) Finding users with the highest number of idle hours.
5) Visualisation.

# Note: The difference of every record is with the difference of 5 mins
If x is user logged in at 10:00 then his second entry will be at 10:05
so every entry is with difference of 5min

# Step1:
Start the hadoop daemons
Start-all.sh
Check if its started or not by using jps command
Load the csv files into hdfs and then merge it
Now start writing the code by creating a ipynb file in vs code

# Step2:
Import the necessary libraries and create a spark session

from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
spark = SparkSession.builder.getOrCreate()

Creating a dataframe by loading a csv file from hdfs:
df=spark.read.csv("hdfs://localhost:9000/spark_sql/merged.csv",header=True)

Considering only these 4 columns from that csv file thus creating new dataframe.
df2 =df.select("user_name","DateTime","keyboard","mouse")



# Step3:
Display users and their record counts.
dfc = df2.groupby("user_name").count()
dfc.show()


# Step4:
Finding users with highest number of average hours
df.createOrReplaceTempView("view1")
df1 = spark.sql("SELECT user_name from view1 WHERE keyboard !=0 OR mouse !=0").groupby("user_name").count()
df1.show(truncate=False)


df3 = df1.createOrReplaceTempView("hour_view")
df4 = spark.sql("SELECT user_name,count,((((count-1)*5)*60)/6) as avg_secs from hour_view")
df4.show(truncate=False)


We need it in hours so ,
from pyspark.sql.functions import *
highest_average_hour = df4.withColumn("highest_average_hours", concat(
           floor(col("avg_secs") % 86400 / 3600), lit(":"),
           floor((col("avg_secs") % 86400) % 3600 / 60), lit(""),
       ))
highest_average_hour.show(truncate=False)

# Step5:
Finding users with lowest number of average hours
from pyspark.sql.functions import *
highest_average_hour = df4.withColumn("highest_average_hours", concat(
           floor(col("avg_secs") % 86400 / 3600), lit(":"),
           floor((col("avg_secs") % 86400) % 3600 / 60), lit(""),
       ))
highest_average_hour.show(truncate=False)
.sort(asc('lowest_average_hours'))

lowest_average_hour.show()

# Step6:
4) Finding users with the highest number of idle hours.

df5 = spark.sql("SELECT user_name FROM view1 WHERE keyboard == 0 and mouse == 0 ").groupBy("user_name").count()
df5.show()

df5.createOrReplaceTempView('idle_hour_view')
df6 = spark.sql("SELECT user_name,count,((((count-1)*5)*60)/6) as average_min from idle_hour_view")
df6.show(truncate=False)

from pyspark.sql.functions import *
idle_hour = df6.withColumn("idle_hours", concat(
           floor(col("average_min") % 86400 / 3600), lit(":"),
           floor((col("average_min") % 86400) % 3600 / 60), lit(""),

idle_hour.show(truncate=False)

