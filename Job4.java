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

public class Job4 {
    //MAPPER
    public static class Mapper4 extends Mapper<Text, Text, Job3Key, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String [] key_splited = key.toString().split(" ");
            IntWritable decade = new IntWritable(Integer.parseInt(key_splited[0]));
            Text word1 = new Text(key_splited[1]);
            Text word2 = new Text(key_splited[2]);
            DoubleWritable likelihood = new DoubleWritable(0);
            if(!key_splited[3].equals("NaN"))
                likelihood= new DoubleWritable(Double.parseDouble(key_splited[3]));
            Job3Key myKey = new Job3Key(new IntWritable(0), word1, word2, decade, likelihood);
            context.write(myKey, new Text(""));
        }
    }
    //REDUCER
    public static class Reducer4 extends Reducer<Job3Key, Text, Text, Text> {
        int current_decade = 0;
        @Override
        public void reduce(Job3Key key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            if(current_decade != key.getDecade())
            {
                current_decade = key.getDecade();
               context.write(new Text("#####DECADE: " + current_decade + "#####"), new Text(""));
            }
            context.write(new Text(key.toString2()), new Text(""));
        }
    }

    //PARTITIONER
    public static class Partitioner4 extends Partitioner<Job3Key, Text> {
        @Override
        public int getPartition(Job3Key key, Text value, int numPartitions) {
            return 0;
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job4");
        job.setJarByClass(Job4.class);

        //Setting Mapper / Reducer  / Partitioner
        job.setMapperClass(Mapper4.class);
        job.setReducerClass(Reducer4.class);
        job.setPartitionerClass(Partitioner4.class);
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
