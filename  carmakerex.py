from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

# Initialize SparkSession
spark = SparkSession.builder.appName("CarMakersAnalysis").getOrCreate()

# Load CSV data
csv_path = "CarMakers.csv"
car_makers_data = spark.read.csv("CarMakers.csv", header=True, inferSchema=True)
car_makers_data.show(10)

# Q.1. Display the total number of car makers
total_car_makers = car_makers_data.count()
print(f"Total Car Makers: {total_car_makers}")

# Q.2.Show the schema and first few rows of the DataFrame
car_makers_data.printSchema()
car_makers_data.show(5)

# Q.3.Find the average price of cars based on fuel type
average_price_by_fuel_type = car_makers_data.groupBy("fuel-type").agg({"price": "avg"}).withColumnRenamed("avg(price)", "average_price")
average_price_by_fuel_type.show()

# Stop the Spark session
spark.stop()
