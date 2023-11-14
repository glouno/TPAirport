from pyspark import SparkContext
from pyspark.sql import SparkSession

# import the dataframe sql data types
from pyspark.sql.types import *
from pyspark.sql import functions as F


#Create SparkSession
spark = SparkSession.builder.appName('TPairport').getOrCreate()

#
# flightSchema describes the structure of the data in the flights.csv file
#
flightSchema = StructType([
  StructField("DayOfMonth", IntegerType(), False),
  StructField("DayOfWeek", IntegerType(), False),
  StructField("Carrier", StringType(), False),
  StructField("OriginAirportID", IntegerType(), False),
  StructField("DestAirportID", IntegerType(), False),
  StructField("DepartureDelay", IntegerType(), False),
  StructField("ArrivalDelay", IntegerType(), False),
])
#
# Use the dataframe reader to read the file and 
#
flights = spark.read.csv('TPairport/raw-flight-data.csv', schema=flightSchema, header=True)
flights.show()

flights2 = spark.read.csv('TPairport/flights.csv', schema=flightSchema, header=True)

'''
#testing the differences:
diff_flights = flights.subtract(flights2)
diff_flights.show()
### Now taking out duplicates
diff_flights_noduplicates = flights.exceptAll(flights2)
diff_flights_noduplicates.show()

flights.describe().show()
'''

#Airports DataFrame, small enough to inferschema on its own
airports = spark.read.csv('TPairport/airports.csv', header=True, inferSchema=True)
airports.show()


cities = airports.select("city", "name")    #remember, 'name' is airport name
cities.limit(5).show()


flightsByOrigin = flights\
.join(airports, flights.OriginAirportID == airports.airport_id)\
.groupBy("city")\
.agg(F.count(F.lit(1)).alias("Count"))\
.orderBy("Count", ascending=False)
#lit() adds a new columns to the DataFrame, in this case filling it with "Count"
flightsByOrigin.limit(5).show()

#Similarly, with Arrival delay per city
highestArrDelay = flights\
    .join(airports, flights.OriginAirportID == airports.airport_id)\
    .groupBy("city")\
    .agg(F.sum("ArrivalDelay").alias("SumArrDelay"))\
    .orderBy("SumArrDelay", ascending=False)

highestArrDelay.limit(10).show()

#Out of curiosity, how many partitions did Spark automatically create for us given my system configuration:
flights.rdd.getNumPartitions()


#End Spark Session and Spark context to avoid error for second launch
spark.stop()