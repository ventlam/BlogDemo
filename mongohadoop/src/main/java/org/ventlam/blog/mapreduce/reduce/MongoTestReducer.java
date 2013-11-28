package org.ventlam.blog.mapreduce.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;

/*
 * Author:  VentLam
 * Email :  ventlcc@gmail.com
 * Blog  :  http://www.ventlam.org/
 * Demo For Hadoop Connect MongoDB.
 */

public class MongoTestReducer extends Reducer<IntWritable,DoubleWritable,IntWritable,BSONWritable>
{
	public void reduce( final IntWritable pKey,
            final Iterable<DoubleWritable> pValues,
            final Context pContext ) throws IOException, InterruptedException{
	  int count = 0;
      double sum = 0.0;
      for ( final DoubleWritable value : pValues ){
          sum += value.get();
          count++;
      }

      final double avg = sum / count;

		BasicBSONObject out = new BasicBSONObject();
		out.put("avg", avg);
		pContext.write(pKey, new BSONWritable(out));
	}
}
