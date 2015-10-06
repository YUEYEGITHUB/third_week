  package com.yueye;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;  
import java.util.StringTokenizer;  
  






import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;  

public class Sort {  
	   
    public static class WordCountMap extends  
            Mapper<LongWritable, Text, RevertKey, Text> {  
  
        //private final IntWritable one = new IntWritable(1);  
    	private IntWritable mapKey = new IntWritable();
    	private Text mapValue = new Text();  
   // 	private Text word = new Text();  
  
        public void map(LongWritable key, Text value, Context context)  
                throws IOException, InterruptedException {  
            String line = value.toString();  
            StringTokenizer token = new StringTokenizer(line);  
          
            while (token.hasMoreTokens()) {  
             
                	mapValue.set(token.nextToken()); 
                	mapKey.set(Integer.parseInt(token.nextToken())); 
                
                	System.out.println(mapKey + " " + mapValue);
                	  RevertKey newKey = new RevertKey(mapKey);
                	context.write(newKey, mapValue);
                }
           
                
              
        }  
    }  
  
    public static class WordCountReduce extends  
            Reducer<  RevertKey , Text, Text, IntWritable> {  
  
        public void reduce(RevertKey key, Iterable<Text> values,  
                Context context) throws IOException, InterruptedException {  
            int sum = 0;  
            for (Text val : values) {  
                context.write(val, key.getKey());  
            }  
            System.out.println("reduce");
            //System.out.println(key + "'s sum = " + sum);
          //  context.write(key, new IntWritable(sum));  
        }  
    }  
    
    //++ revertkey
    public static class RevertKey
    implements WritableComparable<RevertKey> {

private IntWritable key;
public RevertKey(){
  key = new IntWritable();
}

public RevertKey(IntWritable key) {
  this.key = key;
}

public IntWritable getKey() {
  return key;
} 


public void readFields(DataInput in) throws IOException {
  key.readFields(in);
}


public void write(DataOutput out) throws IOException {
  key.write(out);
}


public int compareTo(RevertKey other) {
  return -key.compareTo(other.getKey());
}
}
  
    public static void main(String[] args) throws Exception {  
        Configuration conf = new Configuration();  
        Job job = new Job(conf);  
        job.setJarByClass(WordCount.class);  
        job.setJobName("wordcount");  
  
        job.setMapOutputKeyClass(RevertKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(IntWritable.class);  
  
        job.setMapperClass(WordCountMap.class);  
        job.setReducerClass(WordCountReduce.class);  
  
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
  
        FileInputFormat.addInputPath(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  
        
        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        fs.delete(outputPath, true);
  
        job.waitForCompletion(true);  
    }  
}
