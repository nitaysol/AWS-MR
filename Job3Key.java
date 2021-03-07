import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Job3Key implements WritableComparable<Job3Key> {
    private IntWritable gram;
    private Text word1;
    private Text word2;
    private IntWritable decade;
    private DoubleWritable likelihood;
    public Job3Key(IntWritable gram,Text word1, Text word2, IntWritable decade, DoubleWritable likelihood)
    {
        //0 - decade/ 1 - 1gram / 2 - 2gram
        this.gram = gram;
        this.word1 = word1;
        this.word2 = word2;
        this.decade = decade;
        this.likelihood = likelihood;
    }
    public Job3Key()
    {
        this.gram = new IntWritable(0);
        this.word1 = new Text("");
        this.word2 = new Text("");
        this.decade = new IntWritable(0);
        this.likelihood = new DoubleWritable(0);
    }
    public void readFields(DataInput input) throws IOException
    {
        gram.readFields(input);
        word1.readFields(input);
        word2.readFields(input);
        decade.readFields(input);
        likelihood.readFields(input);
    }
    public void write(DataOutput output) throws IOException
    {
        gram.write(output);
        word1.write(output);
        word2.write(output);
        decade.write(output);
        likelihood.write(output);
    }
    public int compareTo(Job3Key other)
    {
        int decade_compare = this.decade.get() - other.decade.get();
        int likelihood_compare = other.likelihood.compareTo(this.likelihood);
        int word1_compare = this.word1.toString().compareTo(other.word1.toString());
        int word2_compare = this.word2.toString().compareTo(other.word2.toString());
        return decade_compare==0 ? (likelihood_compare ==0 ? (word1_compare==0 ? word2_compare : word1_compare) : likelihood_compare) : decade_compare;
    }
    public int getGram(){return this.gram.get();}
    public String getWord1(){return this.word1.toString();}
    public String getWord2(){return this.word2.toString();}
    public int getDecade(){return this.decade.get();}
    public double getLikelihood(){return this.likelihood.get();}
    @Override
    public String toString(){
        return (getDecade() + " " + getWord1() + " " + getWord2() + " " + getLikelihood());
    }
    public String toString2(){
        return ("Decade: " + getDecade() +", Collocation: " + getWord1() + " " + getWord2() + ", LikeliHood: " + getLikelihood());
    }
}
