# Spark_n_MapReduce

Input Dataset's Link:
https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions/h9gi-nx95

This project contains the following programs:


Task#1: Clean the input data
  1. Keep following columns in the cleaned data
  1.1. Date
  1.2. Borough
  1.3. Zip
  1.4. Number Of Persons Injured
  1.5. Number Of Persons Killed
  1.6. Number Of Pedestrians Injured
  1.7. Number Of Pedestrians Killed
  1.8. Number Of Cyclist Injured
  1.9. Number Of Cyclist Killed
  1.10. Number Of Motorist Injured
  1.11. Number Of Motorist Killed
  1.12. Vehicle Type Code 1
  
  Solution:
	task1.py for task1 (cleaning the data)
	 
	  execution cmd:	 spark-submit --master yarn --deploy-mode client --executor-memory 1g task1.py
	  input folder: /user/data/nypd/NYPD_Motor_Vehicle_WithHeader.txt
	  output folder: cleanedData/
  

Task#2: Process the cleaned data for following information and generate final single output file with all the required numbers
  1. Date on which maximum number of accidents took place.
  2. Borough with maximum count of accident fatality
  3. Zip with maximum count of accident fatality
  4. Which vehicle type is involved in maximum accidents
  5. Year in which maximum Number Of Persons and Pedestrians Injured
  6. Year in which maximum Number Of Persons and Pedestrians Killed
  7. Year in which maximum Number Of Cyclist Injured and Killed (combined)
  8. Year in which maximum Number Of Motorist Injured and Killed (combined)
  

  Solution:
  task2.py for task2 sparkjob:
	
	  execution cmd:	 spark-submit --master yarn --deploy-mode client --executor-memory 1g task2.py
	  input folder: cleanedData/
	  output folder: SparkOutput/


	mapReduce.jar for task2 mapReducejob:

	projectFolder:hadoopMapReduce
	execution cmd: hadoop jar mapReduce.jar mapReduce
	input folder: cleanedData/
	output folder: MapReduceOutput/

	

	
	
