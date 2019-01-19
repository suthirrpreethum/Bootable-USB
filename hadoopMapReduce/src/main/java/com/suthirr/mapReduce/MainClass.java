package com.suthirr.mapReduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MainClass {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {

//		private final static IntWritable one = new IntWritable(1);
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			

			String[] splittedColumns = value.toString().split(",");
			int count = 0;
			
			//task-2.1
			outputKey.set("task2_1");
			//Combining NUMBER OF PERSONS INJURED,NUMBER OF PERSONS KILLED,
			//NUMBER OF PEDESTRIANS INJURED,NUMBER OF PEDESTRIANS KILLED,NUMBER OF CYCLIST INJURED,
			//NUMBER OF CYCLIST KILLED,NUMBER OF MOTORIST INJURED,NUMBER OF MOTORIST KILLED
			//for each row
			for (int i = 3; i <= 10; i++) {
				count += Integer.parseInt(splittedColumns[i]);
			}
			outputValue.set(splittedColumns[0] + "--" + 1);
			context.write(outputKey, outputValue);
			//Writing the output key->task1 value->"date"+"--"+1 (for mapping each row with count 1)

			
			//task-2.2
			outputKey.set("task2_2");
			//Combining  the columns for accident fatality
			count = Integer.parseInt(splittedColumns[4]) + Integer.parseInt(splittedColumns[6])
					+ Integer.parseInt(splittedColumns[8]) + Integer.parseInt(splittedColumns[10]);
			outputValue.set(splittedColumns[1] + "--" + count);
			context.write(outputKey, outputValue);
			//Writing the output key->task2 value->"borough"+"--"+sum the count of accident fatality for each row
			
			
			
			//task-2.3
			outputKey.set("task2_3");
			outputValue.set(splittedColumns[2] + "--" + count);
			context.write(outputKey, outputValue);
			//Writing the output key->task3 value->"zip"+"--"+sum the count of accident fatality for each row
			
			
			
			//task 2.4
			outputKey.set("task2_4");
			count = 0;
			//Combining NUMBER OF PERSONS INJURED,NUMBER OF PERSONS KILLED,
			//NUMBER OF PEDESTRIANS INJURED,NUMBER OF PEDESTRIANS KILLED,NUMBER OF CYCLIST INJURED,
			//NUMBER OF CYCLIST KILLED,NUMBER OF MOTORIST INJURED,NUMBER OF MOTORIST KILLED
			//for each row
			for (int i = 3; i <= 10; i++) {
				count += Integer.parseInt(splittedColumns[i]);
			}
			outputValue.set(splittedColumns[11]+"--"+1);
			context.write(outputKey, outputValue);
			//Writing the output key->task4 value->"vehicle type"+"--"+1 (for mapping each row with count 1)
			
			
			//Seperating year from date
			int year = Integer.parseInt(splittedColumns[0].split("/")[2]);

			
			//task 2.5
			outputKey.set("task2_5");
			outputValue.set(year + "--" + splittedColumns[3] + "--" + splittedColumns[5]);
			context.write(outputKey, outputValue);
			//Writing the output key->task5 value->"year"+"--"+Number Of Persons Injured+"--"Number Of Pedestrians Injured for each row


			//task 2.6
			outputKey.set("task2_6");
			outputValue.set(year + "--" + splittedColumns[4] + "--" + splittedColumns[6]);
			context.write(outputKey, outputValue);
			//Writing the output key->task6 value->"year"+"--"+Number Of Persons Killed+"--"Number Of Pedestrians Killed for each row

			
			//task 2.7
			outputKey.set("task2_7");
			count = Integer.parseInt(splittedColumns[7]) + Integer.parseInt(splittedColumns[8]);
			outputValue.set(year + "--" + count);
			context.write(outputKey, outputValue);
			//Writing the output key->task7 value->"year"+"--"+sum of Number Of Cyclist Injured and Killed for each row
			
			
			//task2.8
			outputKey.set("task2_8");
			count = Integer.parseInt(splittedColumns[9]) + Integer.parseInt(splittedColumns[10]);
			outputValue.set(year + "--" + count);
			context.write(outputKey, outputValue);
			//Writing the output key->task7 value->"year"+"--"+sum of Number Of Motorist Injured and Killed for each row
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			Text outputKey = new Text();
			String maxKey = "";
			int maxValue = 0;
			String keyString = key.toString();
			
			//if condition to segregate task2.5 and task 2.6 from other tasks 
			if (keyString.contains("task2_1") || keyString.contains("task2_2") || keyString.contains("task2_3")
					|| keyString.contains("task2_4") || keyString.contains("task2_7") || keyString.contains("task2_8")) {
				Map<String, Integer> dataMap = new HashMap<String, Integer>();
				
				//to split the actual key from the count with respect to each task and grouping the count for each key with the use of Hashmap
				for (Text val : values) {
					String valString = val.toString();
					String str[] = valString.split("--");
					if (dataMap.containsKey(str[0])) {
						dataMap.put(str[0], dataMap.get(str[0]) + Integer.parseInt(str[1]));
					} else {
						dataMap.put(str[0], Integer.parseInt(str[1]));
					}
				}
				
				//select the max. key with traversing hashmap
				for (Entry<String, Integer> val : dataMap.entrySet()) {
					if (val.getValue() > maxValue) {
						maxKey = val.getKey();
						maxValue = val.getValue();
					}
				}

				result.set(maxValue + "");
				outputKey.set(key.toString()+" "+maxKey);
				context.write(outputKey, result);
				//giving the max count key and value as a result
			} else if (keyString.contains("task2_5") || keyString.contains("task2_6")) {
				int maxValue1 = 0;
				int maxValue2 = 0;
				Map<String, Integer> dateMap1 = new HashMap<String, Integer>();
				Map<String, Integer> dateMap2 = new HashMap<String, Integer>();
				
				//to split the actual key from the count with respect to each task and grouping the count for each key with the use of Hashmap of 2 hashmaps
				for (Text val : values) {
					String valString = val.toString();
					String str[] = valString.split("--");
					if (dateMap1.containsKey(str[0])) {
						dateMap1.put(str[0], dateMap1.get(str[0]) + Integer.parseInt(str[1]));
						dateMap2.put(str[0], dateMap2.get(str[0]) + Integer.parseInt(str[2]));
					} else {
						dateMap1.put(str[0], Integer.parseInt(str[1]));
						dateMap2.put(str[0], Integer.parseInt(str[2]));
					}
				}
				
				//select the max. key with traversing hashmap
				for (Entry<String, Integer> val : dateMap1.entrySet()) {
					if (val.getValue() > maxValue1 && dateMap2.get(val.getKey()) > maxValue2) {
						maxKey = val.getKey();
						maxValue1 = val.getValue();
						maxValue2 = dateMap2.get(val.getKey());
					}
				}

				result.set(maxValue1 + " & " + maxValue2);
				outputKey.set(key.toString()+" "+maxKey);
				context.write(outputKey, result);
				//giving the max count key and value as a result
			}
		}
	}

	public static class CustomPartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String str = key.toString();
			// int age = Integer.parseInt(str[2]);

			return 0;
//			if (numReduceTasks == 0) {
//				return 0;
//			}
//
//			if (str.contains("task2_1")) {
//				return 0;
//			} else if (str.contains("task2_2")) {
//				return 1 % numReduceTasks;
//			} else if (str.contains("task2_3")) {
//				return 2 % numReduceTasks;
//			} else if (str.contains("task2_4")) {
//				return 3 % numReduceTasks;
//			} else if (str.contains("task2_5")) {
//				return 4 % numReduceTasks;
//			} else if (str.contains("task2_6")) {
//				return 5 % numReduceTasks;
//			} else if (str.contains("task2_7")) {
//				return 6 % numReduceTasks;
//			} else if (str.contains("task2_8")) {
//				return 7 % numReduceTasks;
//			} else {
//				return 8 % numReduceTasks;
//			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		//Seting the config. for map reduce program
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(MainClass.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(CustomPartitioner.class);
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//input of output of map and reduce
		FileInputFormat.addInputPath(job, new Path("cleanedData"));
		FileOutputFormat.setOutputPath(job, new Path("MapReduceOutput"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
