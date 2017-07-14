package com.hhh.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by joker on 17-7-13.
 */
public class InfoGain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        if(args.length != 2){
            System.err.println("Usage:InfoGain <input><output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InfoGain");

        job.setJarByClass(InfoGain.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(IGMapper.class);
        job.setReducerClass(IGReducer.class);
        job.setCombinerClass(IGCombiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
    public static class IGMapper extends Mapper<Text, Text, Text, Text> {
        HashMap<String,String> map =new HashMap<String, String>();
        private FileSplit split;
        protected void map(Text key, Text val, Context context)
                throws IOException, InterruptedException{
            split = (FileSplit)context.getInputSplit();
            String pathname = split.getPath().getName().toString();
            String k = key.toString()+":"+pathname;
            if(!map.containsKey(k)) {
                map.put(k, val.toString());
            }
        }
        protected void cleanup(Context context)
            throws IOException, InterruptedException{
            Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
            while(it.hasNext()) {
                Map.Entry<String, String> entry = it.next();
                String word_File = entry.getKey();
                int index = word_File.indexOf(":");
                String word = word_File.substring(0,index);
                String label = entry.getValue();
                context.write(new Text(word+":"+label), new Text("1"));
            }
        }
    }
    public static class IGReducer extends Reducer<Text, Text, DoubleWritable, Text>{

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int[] stat = new int[20];
            int sum = 0;
            for(Text val : values){
                int splitindex = val.toString().indexOf(":");
                int label = Integer.parseInt( val.toString().substring(0, splitindex) );
                int num   = Integer.parseInt( val.toString().substring(splitindex+1) );
                stat[label] += num;
                sum += num;
            }
            double posentropy = 0.0;
            for(int freq: stat){
                if(freq == 0)
                    continue;
                double prob = freq/(double)sum;
                posentropy -= prob * Math.log(prob);
            }
            posentropy *= (sum / 19997.0);


            for(int i = 0; i < 20; i ++){
                int cls = 1000;
                if(i==15)
                    cls = 997;
                stat[i] = cls - stat[i];
            }
            sum = 19997 - sum;
            double negentropy = 0.0;
            for(int freq: stat){
                if(freq == 0)
                    continue;
                double prob = freq/(double)sum;
                negentropy -= prob * Math.log(prob);
            }
            negentropy *= (sum / 19997.0);


            double entropy = negentropy + posentropy;
            context.write( new DoubleWritable(entropy), key);
        }
    }

    public static class IGCombiner extends Reducer<Text, Text, Text , Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for(Text val: values){
                sum += 1;
            }
            int index = key.toString().indexOf(":");
            String word = key.toString().substring(0, index);
            String label = key.toString().substring(index + 1);
            context.write(new Text(word), new Text(label + ":" + sum));
        }
    }
}
