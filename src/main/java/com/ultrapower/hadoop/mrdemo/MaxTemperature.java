package com.ultrapower.hadoop.mrdemo;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {
	 private static final Log LOG =
			    LogFactory.getLog(MaxTemperature.class);
	public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private static final int MISSING = 9999;
		public  MaxTemperatureMapper() {
			
		}
// Map
		@Override
		public   void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			LOG.info("line=================>"+line);
			String year = line.substring(15, 19);
			int airTemperature;
			if (line.charAt(45) == '+') {
				airTemperature = Integer.parseInt(line.substring(46, 50));
			} else {
				airTemperature = Integer.parseInt(line.substring(45, 50));
			}
			String quality = line.substring(50, 51);
			if (airTemperature != MISSING && quality.matches("[01459]")) {
				context.write(new Text(year), new IntWritable(airTemperature));
			}
		}

	}

	public static class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text keyin, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int maxValue = Integer.MIN_VALUE;
			System.out.println("=============reduce=========="+keyin);
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}

			context.write(keyin, new IntWritable(maxValue));
		}
	}

	public static void main(String args[]) throws Exception {
		//数据  0067011990999991950051507004888888889999999N9+00001+9999999999999999999999
		//天气全部数据  ftp://ftp.ncdc.noaa.gov/pub/data/noaa/1901/
		//日志目录 log4j  http://spark2:8042/logs/userlogs/
		//hadoop jar hadoop-0.0.1-SNAPSHOT.jar  com.ultrapower.hadoop.mrdemo.MaxTemperature hdfs://spark1:9000/temperature/mp.txt/mp.txt hdfs://spark1:9000/temperature/output
		LOG.info("args[0]======="+args[0]);
		LOG.info("args[1]======="+args[1]);
		Configuration conf = new Configuration();
		Job job =  Job.getInstance(conf, "Max Temperature");
		job.setJarByClass(MaxTemperature.class);
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

}
