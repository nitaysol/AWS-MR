import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Key implements WritableComparable<Key> {
    private IntWritable gram;
    private Text word1;
    private Text word2;
    private IntWritable decade;
    public Key(IntWritable gram,Text word1, Text word2, IntWritable decade)
    {
        //0 - decade/ 1 - 1gram / 2 - 2gram
        this.gram = gram;
        this.word1 = word1;
        this.word2 = word2;
        this.decade = decade;
    }
    public Key(String s)
    {
        String [] splitedValues = s.split(" ");
        this.gram = new IntWritable(Integer.parseInt(splitedValues[0]));
        switch(gram.get())
        {
            case 0:
                this.word1 = new Text("");
                this.word2 = new Text("");
                this.decade = new IntWritable(Integer.parseInt(splitedValues[1]));
                break;
            case 1:
                this.word1 = new Text(splitedValues[1]);
                this.word2 = new Text("");
                this.decade = new IntWritable(Integer.parseInt(splitedValues[2]));
                break;
            case 2:
                this.word1 = new Text(splitedValues[1]);
                this.word2  = new Text(splitedValues[2]);
                this.decade = new IntWritable(Integer.parseInt(splitedValues[3]));
        }

    }
    public Key()
    {
        this.gram = new IntWritable(0);
        this.word1 = new Text("");
        this.word2 = new Text("");
        this.decade = new IntWritable(0);
    }
    public void readFields(DataInput input) throws IOException
    {
        gram.readFields(input);
        word1.readFields(input);
        word2.readFields(input);
        decade.readFields(input);
    }
    public void write(DataOutput output) throws IOException
    {
        gram.write(output);
        word1.write(output);
        word2.write(output);
        decade.write(output);
    }
    public int compareTo(Key other)
    {
        int decade_compare = this.decade.get() - other.decade.get();
        int word1_compare = (this.word1.toString()).compareTo(other.word1.toString());
        int word2_compare = (this.word2.toString()).compareTo(other.word2.toString());
        return decade_compare==0 ? (word1_compare==0 ? word2_compare : word1_compare) : decade_compare;
    }
    public int getGram(){return this.gram.get();}
    public String getWord1(){return this.word1.toString();}
    public String getWord2(){return this.word2.toString();}
    public int getDecade(){return this.decade.get();}
    public String toString()
    {
        String ret =getGram()+"";
        if(getGram()==1 || getGram()==2)
            ret = ret + " " + getWord1();
        if(getGram()==2)
            ret = ret + " " + getWord2();
        ret = ret + " " + getDecade();
        return ret;

    }
    //Custom functions
    public void swapWords()
    {
        Text temp = new Text(getWord1());
        this.word1 = word2;
        this.word2 = temp;

    }
}
