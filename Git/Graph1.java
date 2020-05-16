import java.io.*;
import java.util.Scanner;
import java.lang.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Graph extends Configured {

	// FirstMapperClass
	public static class FirstMapperClass extends Mapper<Object, Text, LongWritable, LongWritable> {
	
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException
		{
			Scanner s = new Scanner(line.toString()).useDelimiter(",");
			long key2 = s.nextLong();
			long value2 = s.nextLong();
			context.write(new LongWritable(key2), new LongWritable(value2));
			s.close();	
		}
	}


	// FirstReducerClass
	public static class FirstReducerClass extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{
		
		public void reduce(LongWritable key, Iterable<LongWritable> nodes, Context context) throws IOException, InterruptedException
		{
			long count = 0;
			for(LongWritable v:nodes){
				count++;
			};
			context.write(key, new LongWritable(count));	
		}
	}
    	
	// SecondMapperClass
	 public static class SecondMapperClass extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable>
	{
		public void map(LongWritable node, LongWritable count, Context context) throws IOException, InterruptedException
		{
			context.write(count, new LongWritable(1));
		}
	}

	// SecondReducerClass
	public static class SecondReducerClass extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>
	{
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			long sum = 0;
			for(LongWritable v: values) {
				sum += v.get();
			};
			context.write(key, new LongWritable(sum));
		}
	}

			
	public static void main(String [] args) throws Exception {
	
		Job job1 = Job.getInstance();
		job1.setJobName("FirstJob");
		job1.setJarByClass(Graph.class);
		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(LongWritable.class);
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(LongWritable.class);
		job1.setMapperClass(FirstMapperClass.class);
		job1.setReducerClass(FirstReducerClass.class);
		job1.setInputFormatClass(TextInputFormat.class );
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(job1 ,new Path(args[0]));
		FileOutputFormat.setOutputPath(job1 ,new Path("temporary3"));
		boolean success = job1.waitForCompletion(true) ? true : false;
		if(success) {
			Job job2 = Job.getInstance();
			job2.setJobName("SecondJob");
			job2.setJarByClass(Graph.class);
			job2.setOutputKeyClass(LongWritable.class);
			job2.setOutputValueClass(LongWritable.class);
			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(LongWritable.class);
			job2.setMapperClass(SecondMapperClass.class);
			job2.setReducerClass(SecondReducerClass.class);
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.setInputPaths(job2, new Path("temporary3"));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]));
			job2.waitForCompletion(true);
		}
	}

}


