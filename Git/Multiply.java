import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.ReflectionUtils;


class Elem implements Writable {
    short tag;  // 0 for M, 1 for N
    int index;  // one of the indexes (the other is used as a key)
    double value;

	Elem () {}

    Elem ( short t, int index, double value ) {
        this.tag = t;
        this.index = index;
        this.value = value;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeShort(tag);
        out.writeInt(index);
        out.writeDouble(value);
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readShort();
        index = in.readInt();
        value = in.readDouble();
    }
}

class Pair implements WritableComparable<Pair> {
    int i;
    int j;

	Pair () {}

    Pair ( int i, int j ) {
        this.i = i;
        this.j = j;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
    }

    public void readFields ( DataInput in) throws IOException {
        i = in.readInt();
        j = in.readInt();
    }

    public int compareTo(Pair o) {
        if (i > o.i) {
			return 1;
		} 
		else if ( i < o.i) {
			return -1;
		}
		if (j > o.j) {
			return 1;
		} 
		else if (j < o.j) {
			return -1;
		}
		return 0;
    }

    public String toString () { return i + " " + j; }

}

public class Multiply {

    public static class myMapperM extends Mapper<Object,Text,IntWritable,Elem> {
        // @Override
        public void map ( Object key, Text line, Context context )
                throws IOException, InterruptedException {

            Scanner s = new Scanner(line.toString()).useDelimiter(",");
            short tag = 0;

            // Value 1 of string i.e. row
            int index = s.nextInt();

            // Value 2 of string i.e. column
            int columnKey = s.nextInt();

            // Value 3 of string i.e. value
            double value = s.nextDouble();

            context.write(new IntWritable(columnKey) , new Elem( tag, index, value) );

            s.close();

        }
    }


    public static class myMapperN extends Mapper<Object, Text, IntWritable, Elem> {
        // @Override
        public void map ( Object key, Text line, Context context )
            throws IOException, InterruptedException {

            Scanner s = new Scanner(line.toString()).useDelimiter(",");
            short tag = 1;

            // Value 1 of string i.e. row
            int rowKey = s.nextInt();

            // Value 2 of string i.e. column
            int index = s.nextInt();

            // Value 3 of string i.e. value
            double value = s.nextDouble();

            context.write(new IntWritable(rowKey) , new Elem( tag, index, value) );

            s.close();

        }
    }

    public static class myReducer1 extends Reducer<IntWritable, Elem, Pair, DoubleWritable> {
        //@Override
        static Vector<Elem> A = new Vector<Elem>();
        static Vector<Elem> B = new Vector<Elem>();
        public void reduce(IntWritable key, Iterable<Elem> values, Context context)
                throws IOException, InterruptedException {

            A.clear();
            B.clear();

          /*for ( Elem v : values )
            {
                if(v.tag == 0){
                    A.add(v);
                }else{
                    B.add(v);
                }

            }*/
            
            Configuration conf = context.getConfiguration();
            
            for(Elem elem : values) {
			
			Elem e = ReflectionUtils.newInstance(Elem.class, conf);
			ReflectionUtils.copy(conf, elem, e);
			
			if (e.tag == 0) {
				A.add(e);
			} else if(e.tag == 1) {
				B.add(e);
			}
		}   
            
            for( Elem a: A) {
                for( Elem b: B) {

                    context.write( new Pair ( a.index, b.index ), new DoubleWritable( a.value * b.value ));
                }
            }
        }

    }

    public static class myMapper2 extends Mapper<Pair, DoubleWritable, Pair, DoubleWritable> {
        //@Override
        public void map(Pair pair, DoubleWritable dw, Context context)
                throws IOException, InterruptedException {

            context.write( pair, dw );

        }

    }

    public static class myReducer2 extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {
        //@Override
        public void reduce(Pair key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double m = 0.0;
            for ( DoubleWritable v: values )
            {
                m = m + v.get();
            }

            context.write( key, new DoubleWritable(m) );
        }
    }

    public static void main ( String[] args ) throws Exception {

        Job job1 = Job.getInstance();
        job1.setJobName( "job1" );
        job1.setJarByClass( Multiply.class );
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Elem.class);
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setMapperClass(myMapperM.class);
        job1.setMapperClass(myMapperN.class);
        MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class, myMapperM.class);
        MultipleInputs.addInputPath(job1,new Path(args[1]),TextInputFormat.class, myMapperN.class);
        job1.setReducerClass(myReducer1.class );
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        job1.waitForCompletion(true);


        Job job2 = Job.getInstance();
        job2.setJobName( "job2" );
        job2.setJarByClass( Multiply.class );
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(DoubleWritable.class);
		job2.setMapperClass(myMapper2.class);
		job2.setReducerClass(myReducer2.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        job2.waitForCompletion(true);

    }
    
}


