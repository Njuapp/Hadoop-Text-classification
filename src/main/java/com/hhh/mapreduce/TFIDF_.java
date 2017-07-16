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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.*;
import java.util.*;

/**
 * Created by nicbh on 2017/7/15.
 */
public class TFIDF_ {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage:TFIDF <isHDFS><feature><input><output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Path tempPath = new Path("temp");
        FileSystem fs;
        if (args[0].equals("1")) {
            fs = FileSystem.get(conf);
            conf.set("isHDFS", "1");
        } else {
            fs = FileSystem.getLocal(conf);
            conf.set("isHDFS", "0");
        }
        if (fs.exists(new Path(args[3])))
            fs.delete(new Path(args[3]), true);
        if (fs.exists(tempPath))
            fs.delete(tempPath, true);
        conf.set("filenum", Integer.toString(fs.listStatus(new Path(args[2])).length));
//        conf.set("feature", args[1]);

        Job job1 = Job.getInstance(conf, "TF-IDF");
        job1.addCacheFile(new Path(args[1]).toUri());
        job1.setJarByClass(TFIDF_.class);
        job1.setInputFormatClass(KeyValueTextInputFormat.class);
        job1.setMapperClass(TFIDF_.tfidfMapper.class);
        job1.setReducerClass(TFIDF_.tfidf2Reducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        KeyValueTextInputFormat.addInputPath(job1, new Path(args[2]));
        FileOutputFormat.setOutputPath(job1, new Path(args[3]));

//        Job job2 = Job.getInstance(conf, "TF-IDF2");
//        job2.setJarByClass(TFIDF_.class);
//        job2.setInputFormatClass(KeyValueTextInputFormat.class);
//        job2.setMapperClass(TFIDF_.tfidf2Mapper.class);
//        job2.setReducerClass(TFIDF_.tfidf2Reducer.class);
//        job2.setMapOutputKeyClass(Text.class);
//        job2.setMapOutputValueClass(Text.class);
//        job2.setOutputKeyClass(Text.class);
//        job2.setOutputValueClass(Text.class);
//        KeyValueTextInputFormat.addInputPath(job2, tempPath);
//        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        if (job1.waitForCompletion(true)) {
//            System.exit(job2.waitForCompletion(true) ? 0 : 1);
            System.exit(0);
        }
        System.exit(1);
    }

    public static class tfidf2Mapper extends Mapper<Text, Text, Text, Text> {
        private FileSplit split;

        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            split = (FileSplit) context.getInputSplit();

//            Path path = split.getPath();
//            String filename = path.getName().toString();
//            int index = filename.indexOf("-");
//            if (index != -1)
//                filename = filename.substring(0, index);

            String word = key.toString();
            String[] words = value.toString().split("\\s+");
            for (String w : words) {
                String[] v = w.split(":");
                String filename = v[0];
                String val = v[1];
                context.write(new Text(filename), new Text(word + ":" + val));
            }
        }
    }

    public static class tfidf2Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String filename = key.toString();
            filename = filename.substring(filename.indexOf("_") + 1);
            String attr = "";
            for (Text txt : values) {
                attr += txt.toString() + " ";
            }
            context.write(new Text(filename), new Text(attr));
        }
    }

    public static class tfidfMapper extends Mapper<Text, Text, Text, Text> {
        HashMap<String, String[]> wordMap = new HashMap<String, String[]>();
        private FileSplit split;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            /*
            Configuration conf = context.getConfiguration();
            FileSystem fs;
            if (conf.get("isHDFS").equals("1")) {
                fs = FileSystem.get(conf);
            } else {
                fs = FileSystem.getLocal(conf);
            }
            Path path = new Path(conf.get("feature"));
            */
            BufferedReader bufferedReader = new BufferedReader(new FileReader(context.getCacheFiles()[0].toString()));//fs.open(path)));
            String lineTxt;
            int num = 1;
            while ((lineTxt = bufferedReader.readLine()) != null) {
                String[] txt = lineTxt.split("\\s+");
                if (txt.length >= 2)
                    wordMap.put(txt[0], new String[]{Integer.toString(num), txt[1]});
                num++;
            }
            bufferedReader.close();
        }

        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String text = key.toString();
            if (wordMap.containsKey(text)) {
                split = (FileSplit) context.getInputSplit();
                Path path = split.getPath();
                String filename = path.getName().toString();
                int index = filename.indexOf("-");
                if (index != -1)
                    filename = filename.substring(0, index);
                String val = value.toString();
                index = val.lastIndexOf(" ");
                String[] word = wordMap.get(text);
                double tf = (double) Integer.parseInt(val.substring(0, index)) / Integer.parseInt(val.substring(index + 1));
                double idf = Double.parseDouble(word[1]);
                val = tf * idf + "";
                context.write(new Text(filename), new Text(word[0] + ":" + val));
            }
        }
    }

    public static class tfidfReducer extends Reducer<Text, Text, Text, Text> {
        int fileNum;
//        private MultipleOutputs output;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            fileNum = Integer.parseInt(conf.get("filenum"));
//            output = new MultipleOutputs(context);
        }

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, String> valueMap = new HashMap<>();
            for (Text txt : values) {
                String text = txt.toString();
                int index = text.lastIndexOf(":");
                String filename = text.substring(0, index);
                String value = text.substring(index + 1);
                valueMap.put(filename, value);
            }
            Iterator<Map.Entry<String, String>> it = valueMap.entrySet().iterator();
            double idf = Math.log((double) fileNum / valueMap.size());
            String outtxt = "";
            for (Map.Entry<String, String> entry = it.next(); it.hasNext(); entry = it.next()) {
                String enKey = entry.getKey();
                String enValue = Double.parseDouble(entry.getValue()) * idf + " ";
//                output.write(key, new Text(enValue), entry.getKey());
                outtxt += enKey + ":" + enValue;
            }
            context.write(key, new Text(outtxt.trim()));
        }

//        @Override
//        protected void cleanup(Context context) throws IOException, InterruptedException {
//            output.close();
//            super.cleanup(context);
//        }
    }

}

