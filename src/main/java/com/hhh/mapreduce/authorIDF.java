package com.hhh.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by nicbh on 2017/4/30.
 */
public class authorIDF {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // write your code here

        Configuration configuration = new Configuration();
        if (args.length != 3) {
            System.err.println("Usage:authorIDF <input><file_number><output>");
            System.exit(2);
        }

        String path = args[0];

        configuration.set("file_num", String.valueOf(args[1]));
        Job job = Job.getInstance(configuration, "IDF");

        job.setJarByClass(authorIDF.class);
        job.setMapperClass(authorIDF.IDFMapper.class);
        job.setCombinerClass(IDFCombiner.class);
        job.setReducerClass(authorIDF.IDFReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class IDFMapper extends Mapper<Object, Text, Text, Text> {
        //private IntWritable one = new IntWritable(1);
        private IntWritable one = new IntWritable(1);
        private Text strone = new Text("1");
        private Text word = new Text();
        private FileSplit split;

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            split = (FileSplit) context.getInputSplit();
            String pathname = split.getPath().getName().toString();

            int index = pathname.indexOf(".txt");
            if (index == -1)
                index = pathname.indexOf(".TXT");
            pathname = pathname.substring(0, index);

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken() + ":" + pathname);
                context.write(word, strone);
            }
        }
    }


    public static class IDFCombiner extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values) {
                sum += Integer.valueOf(val.toString());
            }

            int splitIndex = key.toString().indexOf(":");
            result.set(key.toString().substring(splitIndex + 1) + ":" + sum + ";");
            key.set(key.toString().substring(0, splitIndex));
            context.write(key, result);
        }
    }

    public static class IDFReducer extends Reducer<Text, Text, Text, Text> {
        private Text value = new Text();
        private HashMap<String, Integer> namemap;
        private int number;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            Configuration conf = context.getConfiguration();
            number = Integer.valueOf(conf.get("file_num"));
        }

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String result = new String();
            namemap = new HashMap<String, Integer>();
            int files = 0, allcount = 1;
            for (Text info : values) {
                String strInfo1 = info.toString();
                result += strInfo1;
                files++;
                int index = strInfo1.lastIndexOf(":");
                String strInfo = strInfo1.substring(index + 1);
                String name = strInfo1.substring(0, index).split("\\d+")[0];
                strInfo = strInfo.substring(0, strInfo.length() - 1);
                try {
                    int count = 0;
                    if (namemap.containsKey(name))
                        count = namemap.get(name);
                    count += Integer.valueOf(strInfo);
                    namemap.put(name, count);
                } catch (NumberFormatException ex) {
                    System.out.println(strInfo1 + " " + strInfo);
                }
                allcount++;
            }
            double idf = Math.log((double) number / allcount);
            for (Map.Entry<String, Integer> entry : namemap.entrySet()) {
                String name = entry.getKey();
                int count = entry.getValue();
                DecimalFormat format = new DecimalFormat("#################.##");
                result = String.valueOf(count) + "-" + format.format(idf);
                value.set(result);
                Text newkey = new Text(name + ", " + key.toString());
                context.write(newkey, value);
            }
        }
    }
}

