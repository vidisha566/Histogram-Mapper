import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {

    //Access package can be private hint provided by IDE.
    public short tag;                 // 0 for a graph vertex, 1 for a group number
    public long group;                // the group where this vertex belongs to
    public long VID;                  // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors


    public Vertex() {
        this.tag = 0;
        this.group = 0;
        this.VID = 0;
        this.adjacent = new Vector<Long>();
    }

    Vertex(short tag, long group, long VID, Vector<Long> adjacent1) {
        this.tag = tag;
        this.group = group;
        this.VID = VID;
        this.adjacent = adjacent1;
    }

    Vertex(short tag, long group) {
        this.tag = tag;
        this.group = group;
        this.VID = 0;
        this.adjacent = new Vector<Long>();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeShort(tag);
        dataOutput.writeLong(group);
        dataOutput.writeLong(VID);
        LongWritable size = new LongWritable(adjacent.size());
        size.write(dataOutput);
        for (Long adj : adjacent) {
            dataOutput.writeLong(adj);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        tag = dataInput.readShort();
        group = dataInput.readLong();
        VID = dataInput.readLong();
        adjacent.clear();
        LongWritable size = new LongWritable();
        size.readFields(dataInput);

        /*for (Long a : adjacent) {
            //a = dataInput.readLong();
            LongWritable node = new LongWritable();
            node.readFields(dataInput);
            adjacent.add(node.get());*/

        for ( int i=0; i<size.get(); i++) {
            LongWritable adj = new LongWritable();
            adj.readFields(dataInput);
            adjacent.add(adj.get());
        }
    }

   /* long getVID() {
        return VID;
    }

    short getTag() { return tag; }

    long getGroup() {
        return group;
    }

    Vector<Long> getAdjacent() { return adjacent; } */

  /* @Override
    public String toString() {
       return tag + " " + group + " " + VID + " " + adjacent;
   }*/


}


public class Graph {

    public static class vertexIDMapper extends Mapper< Object, Text, LongWritable, Vertex> {

        @Override
        public void map ( Object key, Text line, Context context ) throws IOException, InterruptedException {

            /*//short tag = 0;
            //Scanner s = new Scanner(line.toString());
            //String values = s.next();
            String values = line.toString();
            String[] val = values.split(",");
            // getting the 1st value from each line which is the vertex id
            //long VID = s.nextLong();
            long VID = Long.parseLong(val[0]);
            Vector<Long> adjacent = new Vector<Long>();
            for (int i = 1; i < val.length; i++ )
            {
                adjacent.add(Long.parseLong(val[i]));
            }
            context.write(new LongWritable(VID), new Vertex((short) 0,VID,VID,adjacent));*/
            Scanner s = new Scanner(line.toString()).useDelimiter(",");
            long VID = s.nextLong();
            Vector<Long> adjacent = new Vector<Long>();
            while ( s.hasNext() ) {
                adjacent.add(s.nextLong());
            }
            context.write(new LongWritable((VID)), new Vertex((short) 0, VID, VID, adjacent));

        }
    }

    public static class vertexGroupMapper extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {

        @Override
        public void map ( LongWritable key, Vertex vertex, Context context ) throws IOException, InterruptedException {

            context.write(new LongWritable(vertex.VID), vertex);

            for ( Long adj : vertex.adjacent )
            {
                //short tag = 1;
                context.write( new LongWritable(adj), new Vertex ((short) 1, vertex.group ));
            }
        }
    }

    public static class vertexGroupReducer extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {

        @Override
        public void reduce ( LongWritable key, Iterable<Vertex>values, Context context ) throws IOException, InterruptedException {
            Long m = Long.MAX_VALUE;
            Vector<Long> adjacent = new Vector<Long>();
            //short tag = 0;

            for ( Vertex v : values ) {

                if ( v.tag == 0 ) {
                   //adj = (Vector<Long>) v.adjacent.clone();
                    adjacent = (Vector<Long>)v.adjacent.clone();
                }
                m = min(m,v.group);
            }

            context.write(new LongWritable(m), new Vertex((short) 0, m , key.get(), adjacent));
        }

        Long min(Long a, Long b) {
            if(a < b) {
                return a;
            } else {
                return b;
            }
        }
    }

    public static class finalMapper extends Mapper<LongWritable, Vertex, LongWritable, IntWritable> {

        @Override
        public void map ( LongWritable key, Vertex value, Context context ) throws IOException, InterruptedException {
            // You can use the key as it is since it contains group number which is passed by previous reduce
            context.write(key, new IntWritable(1));
        }
    }

    public static class finalReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

        @Override
        public void reduce ( LongWritable key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {

            int sum = 0;
            for ( IntWritable v: values ) {
                sum += v.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }


    public static void main ( String[] args ) throws Exception {

        // Job1 /* ... First Map-Reduce job to read the graph */

        Job job1 = Job.getInstance();
        job1.setJobName("MyJob1");
        job1.setJarByClass(Graph.class);
        job1.setMapperClass(vertexIDMapper.class);
        job1.setNumReduceTasks(0);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/f0"));
        job1.waitForCompletion(true);

        // Job2 /* ... Second Map-Reduce job to propagate the group number */

        for ( short i = 0; i < 5; i++ ) {
            Job job2 = Job.getInstance();
            job2.setJobName("MyJob2 - instance: " + i);
            job2.setJarByClass(Graph.class);
            job2.setMapperClass(vertexGroupMapper.class);
            job2.setReducerClass(vertexGroupReducer.class);
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job2, new Path(args[1] + "/f" + i));
            FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/f" + (i+1)));
            job2.waitForCompletion(true);
        }

        // Job 3 /* ... Final Map-Reduce job to calculate the connected component sizes */

        Job job3 = Job.getInstance();
        job3.setJobName("MyJob3");
        job3.setJarByClass(Graph.class);
        job3.setMapperClass(finalMapper.class);
        job3.setReducerClass(finalReducer.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(IntWritable.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job3, new Path(args[1] + "/f5"));
        FileOutputFormat.setOutputPath(job3, new Path(args[2]));
        job3.waitForCompletion(true);
    }

}
