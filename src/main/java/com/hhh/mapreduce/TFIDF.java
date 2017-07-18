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

public class TFIDF {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // write your code here

        Configuration configuration = new Configuration();
        if (args.length != 4) {
            System.err.println("Usage:TFIDF <num><feature><input><output>");
            System.exit(2);
        }

        String path = args[2];

        Job job = Job.getInstance(configuration, "IDF");

        job.setJarByClass(TFIDF.class);
        job.setMapperClass(TFIDF.IDFMapper.class);
        //job.setCombinerClass(IDFCombiner.class);
        job.setReducerClass(TFIDF.IDFReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //job.addCacheFile(new Path(path).toUri());
        FileInputFormat.addInputPath(job, new Path(args[2]));
        Path tempDir = new Path("temp_" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        FileOutputFormat.setOutputPath(job, tempDir);

        configuration.set("NUMS",args[0]);

        if (job.waitForCompletion(true)) {
            Path temp = new Path(tempDir.toString()+"/part-r-00000");Path out = new Path(args[3]);
            FileSystem f = FileSystem.get(configuration);
            if (f.exists(out))
                f.delete(out, true);
            Job job2 = Job.getInstance(configuration, "Job2");
            job2.setJarByClass(TFIDF.class);
            job2.setMapperClass(TFIDF.Job2Mapper.class);
            job2.setCombinerClass(TFIDF.Job2Combiner.class);
            job2.setReducerClass(TFIDF.Job2Reducer.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.addCacheFile(temp.toUri());
            job2.addCacheFile(new Path(args[1]).toUri());


            FileInputFormat.addInputPath(job2, new Path(args[2]));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));
            FileSystem.get(configuration).deleteOnExit(tempDir);
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }

    public static class IDFMapper extends Mapper<Object, Text, Text, Text> {
        private FileSplit split;
        private HashMap<String, String> wordmap = new HashMap<String, String>();

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            split = (FileSplit) context.getInputSplit();
            String pathname = split.getPath().getName().toString();
            String str = value.toString();

            int index1 = str.indexOf("\t");
            String w = str.substring(0, index1);
            if (!wordmap.containsKey(w)) {
                wordmap.put(w, pathname);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);

            for (Map.Entry entry : wordmap.entrySet()) {
                String w = (String) entry.getKey();
                String filename = (String) entry.getValue();
                context.write(new Text(w), new Text(filename));
            }

        }
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
        private HashMap<String, Integer> featuremap2 = new HashMap<String, Integer>();
        private List<String> temp_values = new ArrayList<String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            Configuration conf = context.getConfiguration();
            System.out.println(conf.get("NUMS"));
            int nums = Integer.parseInt(conf.get("NUMS"));
            URI[] cacheFile = context.getCacheFiles();
            Path path = new Path(cacheFile[0]);
            Path path2 = new Path(cacheFile[1]);
            FileSystem hdfs = FileSystem.get(conf);
            FSDataInputStream dis = hdfs.open(path);
            FSDataInputStream dis2 = hdfs.open(path2);
            BufferedReader in = new BufferedReader(new InputStreamReader(dis));
            BufferedReader in2 = new BufferedReader(new InputStreamReader(dis2));

            String line;
            int n = 1;
            while ((line = in.readLine()) != null) {
                String[] splits = line.split("\\s+");
                featuremap.put(splits[0],Integer.parseInt(splits[1]));
            }
            while ((line = in2.readLine()) != null) {
                String[] splits = line.split("\\s+");
                featuremap2.put(splits[1],n);
                n++;
                if (n > nums)
                    break;
            }
        }

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int wordcount = 0, filecount = 19997;
            String result;
            String results = "";
            for (Text info : values) {
                String strInfo = info.toString();
                int index = strInfo.lastIndexOf(":");
                wordcount += Integer.parseInt(strInfo.substring(index+1));
                temp_values.add(strInfo);
            }
            for (String strInfo : temp_values) {
                int index = strInfo.lastIndexOf(":");

                double tf = (double) Integer.parseInt(strInfo.substring(index+1)) / wordcount;
                //System.out.println(tf);
                String word = strInfo.substring(0,index);
                if (featuremap2.containsKey(word)) {
                    int no = featuremap2.get(word);
                    int num = featuremap.get(word);
                    double idf = Math.log((double) filecount / num);
                    double tf_idf = tf * idf;

                    result = Integer.toString(no) + ":" + Double.toString(tf_idf);
                    results = results + " " + result;
                }
            }
            int index = key.toString().indexOf(":");
            String kind = key.toString().substring(index + 1);
            newKey.set(kind);
            value.set(results);
            context.write(newKey, value);
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
            int index1 = temp.indexOf("\t");
            word.set(temp.substring(0, index1) + "#" + pathname + ":" + temp.substring(index1 + 1));
            context.write(word, one);
        }
    }

    public static class IDFReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int filecount = 0;
            for (Text info : values) {
                filecount++;
            }
            context.write(key, new Text(Integer.toString(filecount)));
        }


    }
}
