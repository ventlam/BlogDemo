package org.ventlam.blog.job;

import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import org.ventlam.blog.map.MongoTestMapper;
import org.ventlam.blog.map.StatPageTimeButtonMapper;
import org.ventlam.blog.reduce.MongoTestReducer;
import org.ventlam.blog.reduce.StatPageTimeButtonReducer;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;


/*
 * Author:	VentLam
 * Email :	ventlcc@gmail.com
 * Blog  :	http://www.ventlam.org/
 * Demo For Hadoop Connect MongoDB.
 * Based on Offical Treasury Yield Calculation
 * https://github.com/mongodb/mongo-hadoop/blob/master/examples/README.md
 */


public class MongoTestJob extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MongoTestJob(),  args);
		System.out.println(exitCode);
	}

	public int run(String[] args) throws Exception {

		// read job conf from xml
		Configuration conf = new Configuration();
		conf.addResource("configuration.xml");
		conf.addResource("mongo-defaults.xml");
		conf.set("mapred.job.priority", JobPriority.VERY_HIGH.toString());
		//Using Distribute Cache
		DistributedCache.createSymlink(conf);
		//parse date , get/set job prefix
		Date date = new Date();

		//Set Job Date
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (0 != otherArgs.length && 1 != otherArgs.length) {

			printUsage();
			System.exit(-1);

		} else if (0 == otherArgs.length) {

		} else if (1 == otherArgs.length) {

			//conf.set("InputDate", dateString);
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
			try {
				date = simpleDateFormat.parse(dateString);
			} catch (ParseException e) {
				try {
					simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
					date = simpleDateFormat.parse(dateString);
				} catch (ParseException pe) {
					pe.printStackTrace();
					System.out.println("ERROR.Parameter 'date' can not be parsed. Please check it.");
					System.exit(-2);
				}
			}
		}
		System.setProperty("path.separator", ":");
		//Using DistributedCache to add Driver Jar File
		DistributedCache.addFileToClassPath(new Path("/user/amap/data/mongo/mongo-2.10.1.jar"), conf);
        DistributedCache.addFileToClassPath(new Path("/user/amap/data/mongo/mongo-hadoop-core_cdh4.3.0-1.1.0.jar"), conf);
		// job conf
		Job job = new Job(conf,"PWD:Mongo-Test-Job");
	 	job.setJarByClass(MongoTestJob.class);
    	job.setMapperClass(MongoTestMapper.class);
    	job.setReducerClass(MongoTestReducer.class);
    	//job map reduce io type
    	job.setMapOutputKeyClass(IntWritable.class);
    	job.setMapOutputValueClass(DoubleWritable.class);
    	job.setOutputKeyClass(IntWritable.class);
    	job.setOutputValueClass(BSONWritable.class);
    	job.setNumReduceTasks(10);

  		//MongoDB IO
    	job.setInputFormatClass(MongoInputFormat.class);
        job.setOutputFormatClass(MongoOutputFormat.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void printUsage() {
		System.out.println("Usage : \n" +
				"\tStatActMonthJob, \t\t\tto do 'today' job, 'today' is the default date.\n" +
				"\tStatActMonthJob date(eg.20130101), \tto do 'date' job.\n"

				);
	}


}
