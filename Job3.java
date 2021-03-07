import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.IOException;

public class Job3 {
    //MAPPER
    public static class Mapper3 extends Mapper<Text, Text, Job3Key, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String [] key_splited = key.toString().split(" ");
            IntWritable gram = new IntWritable(Integer.parseInt(key_splited[0]));
            Text word1 = new Text(key_splited[1]);
            Text word2 = new Text(key_splited[2]);
            IntWritable decade = new IntWritable(Integer.parseInt(key_splited[3]));
            DoubleWritable likelihood = new DoubleWritable(Double.parseDouble(value.toString()));
            Job3Key myKey = new Job3Key(gram, word1, word2, decade, likelihood);
            context.write(myKey, new Text(""));
        }
    }
    //REDUCER
    public static class Reducer3 extends Reducer<Job3Key, Text, Text, Text> {
        int current_decade = -1;
        int counter = 0;
        @Override
        public void reduce(Job3Key key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
                if(current_decade != key.getDecade())
                {
                    current_decade = key.getDecade();
                    counter = 0;
                }
                if(counter<100)
                {
                    counter++;
                    context.write(new Text(key.toString()), new Text(""));
                }
            }
    }

    //PARTITIONER
    public static class Partitioner3 extends Partitioner<Job3Key, Text> {
        @Override
        public int getPartition(Job3Key key, Text value, int numPartitions) {
            return (key.getDecade()+"").hashCode() % numPartitions;
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job3");
        job.setJarByClass(Job3.class);

        //Setting Mapper / Reducer  / Partitioner
        job.setMapperClass(Mapper3.class);
        job.setReducerClass(Reducer3.class);
        job.setPartitionerClass(Partitioner3.class);
        //Setting MAP output
        job.setMapOutputKeyClass(Job3Key.class);
        job.setMapOutputValueClass(Text.class);
        //input - read job1 output
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
