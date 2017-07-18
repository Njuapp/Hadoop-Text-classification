package com.hhh.mapreduce;


import com.google.common.io.LineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.*;

public class wordcount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // write your code here

        Configuration configuration = new Configuration();
        if (args.length != 4) {
            System.err.println("Usage:wordcount <num><feature><input><output>");
            System.exit(2);
        }



        configuration.set("NUMS",args[0]);
        Path temp = new Path(args[1]);Path out = new Path(args[3]);
        FileSystem f = FileSystem.get(configuration);
        if (f.exists(out))
            f.delete(out, true);
        Job job2 = Job.getInstance(configuration, "Job2");
        job2.setJarByClass(wordcount.class);
        job2.setMapperClass(wordcount.Job2Mapper.class);
        //job2.setCombinerClass(wordcount.Job2Combiner.class);
        job2.setReducerClass(wordcount.Job2Reducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.addCacheFile(temp.toUri());
        //job2.addCacheFile(new Path(args[1]).toUri());

        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        job2.waitForCompletion(true) ;

    }


    public static class Job2Combiner extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private Text newKey = new Text();

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            for (Text val : values) {
                num++;
            }
            int splitindex = key.toString().lastIndexOf("#");
            newKey.set(key.toString().substring(splitindex + 1));
            result.set(key.toString().substring(0, splitindex) + ":" + Integer.toString(num));
            context.write(newKey, result);
        }
    }

    public static class Job2Reducer extends Reducer<Text, Text, Text, Text> {
        private Text value = new Text();
        private Text newKey = new Text();
        private HashMap<String, Integer> featuremap = new HashMap<String, Integer>();
        //private HashMap<String, Integer> featuremap2 = new HashMap<String, Integer>();
        private List<String> temp_values = new ArrayList<String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            Configuration conf = context.getConfiguration();
            System.out.println(conf.get("NUMS"));
            int nums = Integer.parseInt(conf.get("NUMS"));
            URI[] cacheFile = context.getCacheFiles();
            Path path = new Path(cacheFile[0]);
            FileSystem hdfs = FileSystem.get(conf);
            FSDataInputStream dis = hdfs.open(path);
            BufferedReader in = new BufferedReader(new InputStreamReader(dis));

            String line;
            int n = 1;
            while ((line = in.readLine()) != null) {
                String[] splits = line.split("\\s+");
                featuremap.put(splits[1],n);
                n++;
                if(n > nums)
                    break;
            }

        }

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //String result;
            //String results = "";
            int nums = 0;
            int index = key.toString().lastIndexOf("_");
            String word = key.toString().substring(index+1);
            if (featuremap.containsKey(word)) {
                int no = featuremap.get(word);
                for (Text info : values) {
                    String strInfo = info.toString();
                    int count = Integer.parseInt(strInfo);
                    nums += count;
                /*String word = key.toString().substring(0,index);
                if (featuremap.containsKey(word)) {
                    int no = featuremap.get(word);

                    result = Integer.toString(no) + ":" + strInfo.substring(index+1);
                    //result = word + ":" + strInfo.substring(index+1);
                    results = results + " " + result;
                }*/
                }
                newKey.set(key.toString().substring(0,index)+"_"+Integer.toString(no));
                value.set(Integer.toString(nums));
                context.write(newKey, value);
            }


        }
    }

    public static class Job2Mapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private FileSplit split;
        private Text one = new Text("1");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            split = (FileSplit) context.getInputSplit();
            String pathname = split.getPath().getName().toString();

            String temp = value.toString();
            String[] splits = temp.split("\\s");
            int index = splits[0].lastIndexOf(":");
            word.set(pathname + "_" + splits[0].substring(0,index));
            one.set(splits[1]);
            context.write(word, one);
        }
    }

}
