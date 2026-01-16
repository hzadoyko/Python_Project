from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("My First PySpark App") \
    .getOrCreate()

# Show the details of the Spark session
spark

# Read a CSV file into a DataFrame
happiness = spark.read.option('header', 'true').csv('happiness.csv')

# Look at the schema of the DataFrame
happiness.printSchema()

# Count the number of rows in the DataFrame
happiness.count()

# Show the first 20 rows of the DataFrame
happiness.show()

# Change data types of happiness scores for calculations later
happiness = happiness.withColumn("happiness2020", col("happiness2020").cast("float"))
happiness = happiness.withColumn("happiness2021", col("happiness2021").cast("float"))

# Read another CSV file into a new DataFrame
life = spark.read.option('header', 'true').csv('expectancy.csv')
life.printSchema()
life.count()
life.show()

# Combine the data from both DataFrames using a join operation
life_happiness = happiness.join(life, 'country', 'inner')
life_happiness.printSchema()
life_happiness.count()
life_happiness.show()

# Add a new column to the combined dataframe to show the average happiness score
life_happiness = life_happiness.withColumn('AvgHappinessScore', (col("happiness2020") + col("happiness2021")) / 2)
life_happiness.show()

# Create a new dataframe for countries averge happiness scores above 6.0
happiest_countries = life_happiness.filter(col('AvgHappinessScore') >= 6.0)

# Show only selected columns from happiest countries dataframe
happiest_countries.select(['country', 'pop2022', 'AvgHappinessScore']).show(50)

# End Spark session
spark.stop()