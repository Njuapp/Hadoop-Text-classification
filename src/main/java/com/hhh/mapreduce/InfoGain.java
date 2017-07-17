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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.*;
import java.util.*;

/**
 * Created by joker on 17-7-13.
 */
public class InfoGain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 3) {
            System.err.println("Usage:InfoGain <input><output><num>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        String tempDir = "temp";
        Path tempPath = new Path(tempDir);

        Job job1 = Job.getInstance(conf, "InfoGain");
        job1.setJarByClass(InfoGain.class);
        job1.setInputFormatClass(KeyValueTextInputFormat.class);
        job1.setMapperClass(IGMapper.class);
        job1.setReducerClass(IGReducer.class);
        //job1.setCombinerClass(IGCombiner.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        KeyValueTextInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, tempPath);


        Job job2 = Job.getInstance(conf, "SortGain");
        job2.setJarByClass(SortGain.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        KeyValueTextInputFormat.addInputPath(job2, tempPath);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        FileSystem fs = FileSystem.get(conf);
        try {
            if (job1.waitForCompletion(true)) {
                int result = job2.waitForCompletion(true) ? 0 : 1;
                fs.delete(tempPath, true);
                if (result == 0) {
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
                            fs.open(new Path(args[1] + "/part-r-00000"))));
                    String lineTxt;
                    ArrayList<String> wordList = new ArrayList<String>();
                    while ((lineTxt = bufferedReader.readLine()) != null) {
                        String[] txt = lineTxt.split("\\s+");
                        if (txt.length >= 2){
                            String word_idf = txt[1];
                            wordList.add(word_idf);

                        }
                    }
                    bufferedReader.close();
                    Writer writer = new OutputStreamWriter(fs.create(new Path(args[1] + "/terms.txt")));
                    int count = 1;
                    int num = Integer.parseInt(args[2]);
                    for (String word_idf : wordList) {
                        int split = word_idf.lastIndexOf(":");
                        String word = word_idf.substring(0, split );
                        String idf = word_idf.substring(split + 1);
                        writer.write(word + "\t" + idf + "\n");
                        if (count >= num)
                            break;
                        count++;
                    }
                    writer.close();
                }
                System.exit(result);
            }
        } finally {
            if (fs.exists(tempPath))
                fs.delete(tempPath, true);
        }
    }

    public static class IGMapper extends Mapper<Text, Text, Text, Text> {

        protected void map(Text key, Text val, Context context)
                throws IOException, InterruptedException {
            int idx = key.toString().lastIndexOf("_");
            String word = key.toString().substring(0, idx);
            FileSplit split = (FileSplit)context.getInputSplit();
            String label = split.getPath().getName();
            context.write(new Text(word), new Text(label));
//            if (!map.containsKey(k)) {
//                map.put(k, val.toString());
//            }
        }

//        protected void cleanup(Context context)
//                throws IOException, InterruptedException {
//            Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
//            while (it.hasNext()) {
//                Map.Entry<String, String> entry = it.next();
//                String word_File = entry.getKey();
//                int index = word_File.indexOf(":");
//                String word = word_File.substring(0, index);
//                String label = entry.getValue();
//                context.write(new Text(word + ":" + label), new Text("1"));
//            }
//        }
    }

    public static class IGReducer extends Reducer<Text, Text, DoubleWritable, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int[] stat = new int[20];
            int sum = 0;
            for (Text val : values) {
                int label = Integer.parseInt(val.toString());
                stat[label] ++;
                sum  ++;
            }

            double posentropy = 0.0;
            for (int freq : stat) {
                if (freq == 0)
                    continue;
                double prob = freq / (double) sum;
                posentropy -= prob * Math.log(prob);
            }
            posentropy *= (sum / 19997.0);
            Double idf = Math.log(19997.0/ (1 + sum));

            for (int i = 0; i < 20; i++) {
                int cls = 1000;
                if (i == 15)
                    cls = 997;
                stat[i] = cls - stat[i];
            }
            sum = 19997 - sum;
            double negentropy = 0.0;
            for (int freq : stat) {
                if (freq == 0)
                    continue;
                double prob = freq / (double) sum;
                negentropy -= prob * Math.log(prob);
            }
            negentropy *= (sum / 19997.0);


            double entropy = negentropy + posentropy;
            context.write(new DoubleWritable(entropy), new Text(key.toString()+":"+ idf.toString()));
        }
    }

}
