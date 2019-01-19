import sys
#import cleanColumns

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

	conf = SparkConf().setAppName("Spark Count Class")
    	sc = SparkContext(conf=conf)
	#input
	cleanData = sc.textFile("cleanedData")
	
	#task2_1
	#mapping each row's date with 1 and reducing all the values for particular date
	task2_1=cleanData.map(lambda data: (data.split(",")[0],1)).reduceByKey(lambda v1,v2:v1 +v2)
	#getting date with max count of accidents
	task2_1=task2_1.max(key = lambda x: x[1])
	
	
	#task2_2
	#mapping each row's borough with sum of accident fatality and reducing all the values for particular borough
	task2_2 = cleanData.map(lambda data: (data.split(",")[1],int(data.split(",")[4])+int(data.split(",")[6])+int(data.split(",")[8])+int(data.split(",")[10]))).reduceByKey(lambda v1,v2:v1 +v2)
	#getting borough with max count of accident fatality
	task2_2 =task2_2.max(key = lambda x: x[1])
	
	
	#task2_3
	#mapping each row's zip with sum of accident fatality and reducing all the values for particular zip
	task2_3 = cleanData.map(lambda data: (data.split(",")[2],int(data.split(",")[4])+int(data.split(",")[6])+int(data.split(",")[8])+int(data.split(",")[10]))).reduceByKey(lambda v1,v2:v1 +v2)
	#getting zip with max count of accident fatality
	task2_3 =task2_3.max(key = lambda x: x[1])
	
	
	#task2_4
	#mapping each row's vehicle type with sum of accidents and reducing all the values for particular vehicle type
	task2_4 = cleanData.map(lambda data: (data.split(",")[11],1)).reduceByKey(lambda v1,v2:v1 +v2)
	#getting vehicle type with max count of accidents
	task2_4=task2_4.max(key = lambda x: x[1])
	
	#task2_5
	#mapping each row's year with Number Of Persons and Pedestrians Injured
	task2_5 = cleanData.map(lambda data: (data.split(",")[0].split("/")[2],str(data.split(",")[3])+"-"+str(data.split(",")[5])))
	#reducing all the values for particular year
	task2_5=task2_5.reduceByKey(lambda v1,v2:str(int(v1.split('-')[0])+int(v2.split('-')[0]))+"-"+str(int(v1.split('-')[1])+int(v2.split('-')[1])))
	#getting year with max Number Of Persons and Pedestrians Injured
	task2_5=task2_5.max(key = lambda x: x[1].split('-')[0])
	
	
	#task2_6
	#mapping each row's year with Number Of Persons and Pedestrians Killed
	task2_6 = cleanData.map(lambda data: (data.split(",")[0].split("/")[2],str(data.split(",")[4])+"-"+str(data.split(",")[6])))
	#reducing all the values for particular year
	task2_6=task2_6.reduceByKey(lambda v1,v2:str(int(v1.split('-')[0])+int(v2.split('-')[0]))+"-"+str(int(v1.split('-')[1])+int(v2.split('-')[1])))
	#getting year with max Number Of Persons and Pedestrians Killed
	task2_6=task2_6.max(key = lambda x: x[1].split('-')[0])
	
	#task2_7
	#mapping each row's year with sum of Number Of Cyclist Injured and Killed(total)
	task2_7 = cleanData.map(lambda data: (data.split(",")[0].split("/")[2],int(data.split(",")[7])+int(data.split(",")[8]))).reduceByKey(lambda v1,v2:v1 +v2)
	#getting year with max Number Of Cyclist Injured and Killed(total)
	task2_7=task2_7.max(key = lambda x: x[1])
	
	
	#task2_8
	#mapping each row's year with sum of Number Of Motorist Injured and Killed(total)
	task2_8 = cleanData.map(lambda data: (data.split(",")[0].split("/")[2],int(data.split(",")[9])+int(data.split(",")[10]))).reduceByKey(lambda v1,v2:v1 +v2)
	#getting year with max Number Of Motorist Injured and Killed(total)
	task2_8=task2_8.max(key = lambda x: x[1])
	
	s='task1 '+str(task2_1), 'task2 '+str(task2_2),'task3 '+str(task2_3),'task4 '+str(task2_4),'task5 '+str(task2_5),'task6 '+str(task2_6),'task7 '+str(task2_7),'task8 '+str(task2_8)
	rdd=sc.parallelize(s)
	rdd=rdd.coalesce(1)
	//saving to csv
	rdd.saveAsTextFile('SparkOutput/')

