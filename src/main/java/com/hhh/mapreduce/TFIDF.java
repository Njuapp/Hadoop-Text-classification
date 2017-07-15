package com.hhh.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.*;
import java.util.*;

/**
 * Created by nicbh on 2017/7/15.
 */
public class TFIDF {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage:TFIDF <isHDFS><feature><input><output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.set("isHDFS", (args[0].equals("1")) ? "1" : "0");
        conf.set("feature", args[1]);
        Job job = Job.getInstance(conf, "TF-IDF");

        job.setJarByClass(TFIDF.class);
        job.setMapperClass(TFIDF.tfidfMapper.class);
        job.setReducerClass(TFIDF.tfidfReducer.class);
//        job.setCombinerClass(TFIDF.tfidfCombiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class tfidfMapper extends Mapper<Object, Text, Text, IntWritable> {
        HashMap<String, String> wordMap = new HashMap<String, String>();
        private FileSplit split;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            FileSystem fs;
            if (conf.get("isHDFS").equals("1")) {
                fs = FileSystem.get(conf);
            } else {
                fs = FileSystem.getLocal(conf);
            }
            Path path = new Path(conf.get("feature"));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)));
            String lineTxt;
            while ((lineTxt = bufferedReader.readLine()) != null) {
                String[] txt = lineTxt.split("\\s+");
                if (txt.length >= 2)
                    wordMap.put(txt[0], txt[1]);
            }
            bufferedReader.close();
        }

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            split = (FileSplit) context.getInputSplit();
            Path path = split.getPath();
            String pathname = path.getName().toString();
            String filename = pathname + "_" + classMap.get(path.getParent().getName().toString());

            context.write(new Text(word + ":" + filename), new IntWritable(1));

        }
    }


    public static class tfidfReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String text = key.toString();
            int index = text.lastIndexOf(":");
            String word = text.substring(0, index);
            String filename = text.substring(index + 1);
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }
            context.write(new Text(word), new IntWritable(sum));
        }

    }

}
