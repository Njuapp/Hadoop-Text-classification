package com.hhh.mapreduce;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;


/**
 * Created by nicbh on 2017/7/12.
 */
public class preprocess {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage:preprocess <isHDFS><map><input><output>, isHDFS is 0 or 1");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        FileSystem fs;
        if (args[0].equals("1")) {
            fs = FileSystem.get(conf);
        } else {
            fs = FileSystem.getLocal(conf);
        }
        Path mapFile = new Path(args[1]);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(mapFile)));
        String lineTxt;
        String outTxt = "";
        while ((lineTxt = bufferedReader.readLine()) != null) {
            String[] txt = lineTxt.split("\\s+");
            if (txt.length >= 2)
                outTxt += txt[0] + ":" + txt[1] + "\t";
        }
        conf.set("classmap", outTxt);
        Job job = Job.getInstance(conf, "preprocess");

        job.setJarByClass(preprocess.class);
        job.setMapperClass(preprocess.preMapper.class);
        job.setReducerClass(preprocess.preReducer.class);
        job.setCombinerClass(preprocess.preCombiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1000);

        Path file = new Path(args[2]);
        FileStatus[] classFile = fs.listStatus(file);
        for (FileStatus f : classFile) {
            Path path = f.getPath();
            String classname = path.getName();
            if (!classname.equals(".DS_Store")) {
                FileInputFormat.addInputPath(job, path);
            }
        }
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class preMapper extends Mapper<Object, Text, Text, IntWritable> {
        HashMap<String, String> classMap = new HashMap<String, String>();
        EnglishAnalyzer analyzer = new EnglishAnalyzer(Version.LUCENE_46);
        private FileSplit split;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            Configuration conf = context.getConfiguration();
            String outTxt = conf.get("classmap");
            String[] classes = outTxt.split("\t");
            for (String item : classes) {
                String[] cla = item.split(":");
                classMap.put(cla[0], cla[1]);
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            split = (FileSplit) context.getInputSplit();
            Path path = split.getPath();
            String pathname = path.getName().toString();
            String filename = pathname + "_" + classMap.get(path.getParent().getName().toString());

            Reader reader = new StringReader(value.toString());
            TokenStream ts = analyzer.tokenStream("fieldName", reader);
            ts.reset();
            while (ts.incrementToken()) {
                CharTermAttribute ca = ts.getAttribute(CharTermAttribute.class);
                String word = ca.toString();

                context.write(new Text(word + ":" + filename), new IntWritable(1));
            }
            ts.close();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            classMap = null;
            analyzer = null;
            split = null;
        }
    }


    public static class preReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private MultipleOutputs output;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            output = new MultipleOutputs(context);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String text = key.toString();
            int index = text.lastIndexOf(":");
            String word = text.substring(0, index);
            String filename = text.substring(index + 1);
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }
            output.write(new Text(word), new IntWritable(sum), filename);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            output.close();
        }
    }

    public static class preCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}

    /*


        try {
            HashMap<String, String> classMap = new HashMap<>();
            if (args[0].equals("1")) {
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(conf);
//            FileSystem fs = FileSystem.getLocal(conf);
//            if (args[0].equals("1")) {
//                fs = FileSystem.get(conf);
//            }
                Path mapFile = new Path(args[1]);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(mapFile)));
                String lineTxt;
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    String[] txt = lineTxt.split("\\s+");
                    classMap.put(txt[0], txt[1]);
                }

                String input = args[2];
                String output = args[3];
                Path outputDir = new Path(output);
                if (!fs.exists(outputDir)) {
                    fs.mkdirs(outputDir);
                }
                Path file = new Path(input);
                FileStatus[] classFile = fs.listStatus(file);
                int fileCount = 0;
                for (FileStatus f : classFile) {
                    Path path = f.getPath();
                    String classname = path.getName();
                    if (!classname.equals(".DS_Store")) {
                        String classNum = classMap.get(classname);
                        FileStatus[] textFile = fs.listStatus(path);
                        System.out.println(classname + "\t" + textFile.length);
                        for (FileStatus textfile : textFile) {
                            Path textPath = textfile.getPath();
                            String filename = textPath.getName();
                            if (!filename.equals(".DS_Store")) {
                                Reader reader = new InputStreamReader(fs.open(textPath));
                                EnglishAnalyzer analyzer = new EnglishAnalyzer(Version.LUCENE_46);
                                TokenStream tokenStream = analyzer.tokenStream("contents", reader);
                                TokenStream ts = analyzer.tokenStream("fieldName", reader);
                                ts.reset();
                                String filePathname = output + "/" + Integer.toString(fileCount);
                                Writer writer = new OutputStreamWriter(fs.create(new Path(filePathname)));
                                while (ts.incrementToken()) {
                                    CharTermAttribute ca = ts.getAttribute(CharTermAttribute.class);
                                    writer.write(ca.toString() + "\t" + classNum + "\n");
                                }
                                reader.close();
                                writer.close();
                                fileCount++;
                            }
                        }
                    }
                }
            } else {
                File mapFile = new File(args[1]);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(mapFile)));
                String lineTxt;
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    String[] txt = lineTxt.split("\\s+");
                    classMap.put(txt[0], txt[1]);
                }

                String input = args[2];
                String output = args[3];
                File outputDir = new File(output);
                if (!outputDir.exists()) {
                    outputDir.mkdirs();
                }
                File file = new File(input);
                File[] classFile = file.listFiles();
                int fileCount = 0;
                for (File f : classFile) {
                    String classname = f.getName();
                    if (!classname.equals(".DS_Store")) {
                        String classNum = classMap.get(classname);
                        File[] textFile = f.listFiles();
                        System.out.println(classname + "\t" + textFile.length);
                        for (File textfile : textFile) {
                            String filename = textfile.getName();
                            if (!filename.equals(".DS_Store")) {
                                Reader reader = new FileReader(textfile);
                                EnglishAnalyzer analyzer = new EnglishAnalyzer(Version.LUCENE_46);
                                TokenStream tokenStream = analyzer.tokenStream("contents", reader);
                                TokenStream ts = analyzer.tokenStream("fieldName", reader);
                                ts.reset();
                                FileWriter writer = new FileWriter(new File(output + "/" + Integer.toString(fileCount)));
                                while (ts.incrementToken()) {
                                    CharTermAttribute ca = ts.getAttribute(CharTermAttribute.class);
                                    writer.write(ca.toString() + "\t" + classNum + "\n");
                                }
                                writer.close();
                                fileCount++;
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    */
