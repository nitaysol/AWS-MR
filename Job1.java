import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Job1 {
    //MAPPER
    public static class Mapper1 extends Mapper<LongWritable, Text, Key, LongWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] words;
                String word1;
                String word2;
                Configuration conf = context.getConfiguration();
                String language = conf.get("lan");
                int gram;
                //split by \t -> [0] = ngram, [1] = year, [2] = occurrences
                String[] valueSplited = value.toString().split("\t");

                //set occurrences
                LongWritable occurrences = new LongWritable(Long.parseLong(valueSplited[2]));

                //parsing year to decade
                IntWritable decade = new IntWritable(((Integer.parseInt(valueSplited[1])) / 10) * 10);

                //set words
                words = valueSplited[0].split(" ");
                if (words.length == 1) {
                    gram = 1;
                    word1 = words[0].toLowerCase();
                    word2 = "";
                } else {
                    gram = 2;
                    word1 = words[0].toLowerCase();
                    word2 = words[1].toLowerCase();
                }
                //Building key objects;
                Key outputKey = new Key(new IntWritable(gram), new Text(word1), new Text(word2), decade);
                Key decadeKey = new Key(new IntWritable(0), new Text(""), new Text(""), decade);
                if (!StopWords.contains_stop_words(outputKey.getWord1(), outputKey.getWord2(), language.equals("heb"))) {
                    //Writing Key(word1, word2, decade);
                    context.write(outputKey, occurrences);
                    if (gram == 1)
                        context.write(decadeKey, occurrences);
                }
            }
            catch(Exception e)
            {
                System.out.println("Error occurred on this value");
            }
        }
    }

    //REDUCER
    public static class Reducer1 extends Reducer<Key, LongWritable, Text, Text> {
        private long word1_count = 0;
        @Override
        public void reduce(Key key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value : values) {
                count += value.get();
            }
            switch(key.getGram())
            {
                case 0:
                    context.write(new Text(key.toString()), new Text("" + count));
                    break;
                case 1:
                    word1_count = count;
                    context.write(new Text(key.toString()), new Text("" + count));
                    break;
                case 2:
                    context.write(new Text(key.toString()), new Text(word1_count + " " +count));
            }

        }
    }

    //PARTIONER
    public static class Partitioner1 extends Partitioner<Key, LongWritable> {
        @Override
        public int getPartition(Key key, LongWritable value, int numPartitions) {
            return (key.getDecade()+"").hashCode() % numPartitions;
        }
    }

    //COMBINER
    public static class Combiner1 extends Reducer <Key,LongWritable,Key,LongWritable> {
        @Override
        public void reduce (Key key ,Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value : values) {
                count += value.get();
            }
            context.write(key, new LongWritable(count));
        }
    }

    //MAIN
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //setting language
        conf.set("lan", args[3]);
        Job job = Job.getInstance(conf, "Job1");
        job.setJarByClass(Job1.class);

        //Setting Mapper / Reducer / Combiner / Partitioner
        job.setMapperClass(Mapper1.class);
        //job.setCombinerClass(Combiner1.class);
        job.setReducerClass(Reducer1.class);
        job.setPartitionerClass(Partitioner1.class);
        //Setting MAP output
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(LongWritable.class);
        //input - reading both ngram1 and 2
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));

        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        //wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
