package org.ventlam.blog.mapreduce.map;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

/*
 * Author:	VentLam
 * Email :	ventlcc@gmail.com
 * Blog  :	http://www.ventlam.org/
 * Demo For Hadoop Connect MongoDB.
 */

public class MongoTestMapper extends Mapper<Object,BSONObject, IntWritable, DoubleWritable> {

		@Override
		public void map(final Object pkey, final BSONObject pvalue,final Context context)
		{
			final int year = ((Date)pvalue.get("_id")).getYear()+1990;
			double bdyear  = ((Number)pvalue.get("bc10Year")).doubleValue();
			try {
				context.write( new IntWritable( year ), new DoubleWritable( bdyear ));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
}
