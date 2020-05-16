import java.io.*;
import java.util.*;
import java.util.Scanner;
import java.util.Vector;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

/* single color intensity */
class Color implements WritableComparable {
    public short type;       /* red=1, green=2, blue=3 */
    public short intensity;  /* between 0 and 255 */
    /* need class constructors, toString, write, readFields, and compareTo methods */
	Color() {}
	
	Color( short t, short i ) {
		type = t;
		intensity = i;
	}
	public void write ( DataOutput out ) throws IOException {
        out.writeShort(type);
        out.writeShort(intensity);
    }

    public void readFields ( DataInput in ) throws IOException {
        type = in.readShort();
        intensity = in.readShort();
    }

   @Override
    public String toString () { return type+" "+intensity; }

   public int compareTo(Object o){
	   
	   Color x = (Color) o;
	   
    if (x.type == type)
    {
	
	if(x.intensity==intensity)
	{
		return 0;
	}
	else if (x.intensity > intensity)
	{
		return -1;
	}
	else
	{
		return 1;
	}
   }
    else if (x.type > type)
    {
    	return -1;
    }
    else
    {
    	return 1;
    }
}
}

public class Histogram {
	
	public static class HistogramMapper extends Mapper<Object,Text,Color,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
        	Scanner s = new Scanner(value.toString()).useDelimiter(",");
            short red = s.nextShort();
            short green = s.nextShort();
            short blue = s.nextShort();
            Color a = new Color((short)1, red);
            Color b = new Color((short)2, green);
            Color c = new Color((short)3, blue);
            context.write( a, new IntWritable(1));
            context.write( b,new IntWritable(1));
            context.write( c, new IntWritable(1) );
            s.close();
            /* write your mapper code */
        }
    }
	
	public static class HistogramInMapper extends Mapper<Object,Text,Color,IntWritable>
	{
		Hashtable<Color,IntWritable> H;
		@Override
		public void setup( Context context ) throws IOException, InterruptedException{
			H = new Hashtable<Color,IntWritable>();
		}
		@Override
		public void map ( Object key, Text value, Context context) throws IOException, InterruptedException {
        	Scanner s = new Scanner(value.toString()).useDelimiter(",");
            short red = s.nextShort();
            short green = s.nextShort();
            short blue = s.nextShort();
            Color a = new Color((short)1, red);
            Color b = new Color((short)2, green);
            Color c = new Color((short)3, blue);
            	if (H.get(a) == null)
            		{
            		H.put(a, new IntWritable(1));
            		}
                else if (H.get(b) == null)
                	{
                	H.put(b, new IntWritable(1));
                	}
            	else 
            		{
            		H.put(c, new IntWritable(1));
            		}
		}
		@Override
		public void cleanup(Context context ) throws IOException, InterruptedException {
			Set<Color> keys = H.keySet();
			for (Color key: keys) {
				context.write(key,H.get(key));
			}
		}
		}
            /* write your mapper code */
	public static class HistogramCombiner extends Reducer<Color,IntWritable,Color,IntWritable> {
        @Override
        public void reduce ( Color key, Iterable<IntWritable> values, Context context )
        		throws IOException, InterruptedException {
            /* write your reducer code */
     int sum = 0;
     long count = 0;
     for (IntWritable v: values) {
         sum += v.get();
         count++;
     };
     context.write(key,new IntWritable(sum));
}
    }
 

    public static class HistogramReducer extends Reducer<Color,IntWritable,Color,LongWritable> {
        @Override
        public void reduce ( Color key, Iterable<IntWritable> values, Context context )
        		throws IOException, InterruptedException {
            /* write your reducer code */
     int sum = 0;
     long count = 0;
     for (IntWritable v: values) {
         sum += v.get();
         count++;
     };
     context.write(key,new LongWritable(sum));
}
    }
       
    public static void main ( String[] args ) throws Exception {
        /* write your main program code */
    	Job job1 = Job.getInstance();
        job1.setJobName("MyJob1");
        job1.setJarByClass(Histogram.class);
        job1.setOutputKeyClass(Color.class);
        job1.setOutputValueClass(LongWritable.class);
        job1.setMapOutputKeyClass(Color.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setMapperClass(HistogramMapper.class);
        job1.setCombinerClass(HistogramCombiner.class);
        job1.setReducerClass(HistogramReducer.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]));
        job1.waitForCompletion(true);
        
        Job job2 = Job.getInstance();
        job2.setJobName("MyJob2");
        job2.setJarByClass(Histogram.class);
        job2.setOutputKeyClass(Color.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setMapOutputKeyClass(Color.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setMapperClass(HistogramInMapper.class);
        job2.setReducerClass(HistogramReducer.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path(args[0]));
        FileOutputFormat.setOutputPath(job2,new Path(args[1]+"2"));
        job2.waitForCompletion(true);
   }
}



