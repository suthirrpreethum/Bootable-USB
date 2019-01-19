from pyspark.sql import SparkSession

if __name__ == "__main__":

	#create spark session
	sparkSession = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()
	
	#getting the input
	inputFile=sparkSession.read.option("header", "true").option("delimiter", ",").option("inferSchema", "true").csv("/user/data/nypd/NYPD_Motor_Vehicle_WithHeader.txt")
	
	#selecting particular columns
	inputFile=inputFile.select('#DATE','BOROUGH','ZIP CODE','NUMBER OF PERSONS INJURED','NUMBER OF PERSONS KILLED','NUMBER OF PEDESTRIANS INJURED','NUMBER OF PEDESTRIANS KILLED','NUMBER OF CYCLIST INJURED','NUMBER OF CYCLIST KILLED','NUMBER OF MOTORIST INJURED','NUMBER OF MOTORIST KILLED','VEHICLE TYPE CODE 1')
	
	#removing the records which has empty columns
	inputFile=inputFile.na.drop()
	
	#writing it as csv
	inputFile.coalesce(1).write.csv("cleanedData")

