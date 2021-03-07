import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Job2 {
    //MAPPER
    public static class Mapper2 extends Mapper<Text, Text, Key, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Key myKey = new Key(key.toString());
            if(myKey.getGram()==2)
                myKey.swapWords();
            context.write(myKey, value);
        }
    }
    //REDUCER
    public static class Reducer2 extends Reducer<Key, Text, Text, Text> {
        private long N = 0;
        private long word2_count = 0;
        @Override
        public void reduce(Key key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Text value = values.iterator().next();
            switch(key.getGram())
            {
                case 0:
                    N = Long.parseLong(value.toString());
                    break;
                case 1:
                    word2_count = Long.parseLong(value.toString());
                    break;
                case 2:
                    long word1_count = Long.parseLong(value.toString().split(" ")[0]);
                    long word1A2_count = Long.parseLong(value.toString().split(" ")[1]);
                    key.swapWords(); //swap words again because swapped on map1;
                    context.write(new Text(key.toString()),
                            new Text(calculateLikelihood(word1_count, word2_count, word1A2_count, N)+""));
            }
        }
        private double calculateLikelihood(long w1c, long w2c, long w12c, long decadeCount)
        {
            try {
                double c1 = w1c;
                double c2 = w2c;
                double c12 = w12c;
                double N = decadeCount;
                double p = c2 / N;
                double p1 = c12 / c1;
                double p2 = (c2 - c12) / (N - c1);
                double L1 = Math.log10(L(c12, c1, p));
                double L2 = Math.log10(L(c2 - c12, N - c1, p));
                double L3 = Math.log10(L(c12, c1, p1));
                double L4 = Math.log10(L(c2 - c12, N - c1, p2));
                return ((L1 + L2 - L3 - L4) * -2);
            }
            catch(Exception e) {
                return 0;
            }

        }
        private double L(double k, double n, double x)
        {
            return Math.pow(x, k) * Math.pow(1-x, n-k);
        }
    }
    //PARTIONER
    public static class Partitioner2 extends Partitioner<Key, Text> {
        @Override
        public int getPartition(Key key, Text value, int numPartitions) {
            return (key.getDecade()+"").hashCode() % numPartitions;
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job2");
        job.setJarByClass(Job2.class);

        //Setting Mapper / Reducer  / Partitioner
        job.setMapperClass(Mapper2.class);
        job.setReducerClass(Reducer2.class);
        job.setPartitionerClass(Partitioner2.class);
        //Setting MAP output
        job.setMapOutputKeyClass(Key.class);
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
