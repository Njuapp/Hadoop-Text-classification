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
import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by nicbh on 2017/4/30.
 */
public class TFIDF {
    public static String getFeature(Path path,int nums) throws IOException{

            StringBuffer sb = new StringBuffer();
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            FSDataInputStream dis = hdfs.open(path);
            BufferedReader in = new BufferedReader(new InputStreamReader(dis));
            //FileSystem
            String line = null;
            int n = 0;
            while((line = in.readLine()) != null) {
                sb.append(line.trim());
                sb.append("#");
                n++;
                if( n>= nums )
                    break;
            }
            return sb.toString().trim();


    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // write your code here

        Configuration configuration = new Configuration();
        if (args.length != 4) {
            System.err.println("Usage:TFIDF <num><feature><input><output>");
            System.exit(2);
        }

        String path = args[2];
        try {
            String features = getFeature(new Path(args[1]), Integer.parseInt(args[0]));
            configuration.set("FEATURE1", features);
        }
        catch(IOException e){
            e.printStackTrace();
        }


        Job job = Job.getInstance(configuration, "IDF");

        job.setJarByClass(TFIDF.class);
        job.setMapperClass(TFIDF.IDFMapper.class);
        //job.setCombinerClass(IDFCombiner.class);
        job.setReducerClass(TFIDF.IDFReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[2]));
        Path tempDir = new Path("temp_"+Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        FileOutputFormat.setOutputPath(job, tempDir);

        if(job.waitForCompletion(true)) {
            try {
                String temp = tempDir.toString()+"/part-r-00000";
                String features = getFeature(new Path(temp), Integer.parseInt(args[0]));
                //System.out.println(features);
                configuration.set("FEATURE2", features);
            }
            catch(IOException e){
                e.printStackTrace();
            }

            Path out = new Path(args[3]);
            FileSystem f = FileSystem.get(configuration);
            if(f.exists(out))
                f.delete(out,true);
            Job job2 = Job.getInstance(configuration,"Job2");
            job2.setMapperClass(Job2Mapper.class);
            job2.setCombinerClass(Job2Combiner.class);
            job2.setReducerClass(Job2Reducer.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path(args[2]));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));
            FileSystem.get(configuration).deleteOnExit(tempDir);
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }

    public static class IDFMapper extends Mapper<Object, Text, Text, Text> {
        //private IntWritable one = new IntWritable(1);
        private IntWritable one = new IntWritable(1);
        //private Text strone = new Text("1");
        private Text value = new Text();
        private Text word = new Text();
        private FileSplit split;
        private HashMap<String,String> wordmap = new HashMap<String,String>();

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //StringTokenizer itr = new StringTokenizer(value.toString());

            split = (FileSplit) context.getInputSplit();
            String pathname = split.getPath().getName().toString();

            int index = pathname.indexOf(".txt");
            if (index == -1)
                index = pathname.indexOf(".TXT");
            if(index != -1)
                pathname = pathname.substring(0, index);

            String str = value.toString();
            //while (itr.hasMoreTokens()) {
                int index1 = str.indexOf("\t");
            String w = str.substring(0,index1);
            if(!wordmap.containsKey(w)) {
                wordmap.put(w, pathname);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);

            for(Map.Entry entry : wordmap.entrySet()){
                String w = (String)entry.getKey();
                String filename = (String)entry.getValue();
                context.write(new Text(w),new Text(filename));
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
            int splitindex = key.toString().indexOf(":");
            //int splitindex2 = key.toString().lastIndexOf(":");
            newKey.set(key.toString().substring(splitindex+1));
            result.set(key.toString().substring(0,splitindex)+":"+Integer.toString(num));
            context.write(newKey,result);
        }
    }

    public static class Job2Reducer extends Reducer<Text, Text, Text, Text> {
        private Text value = new Text();
        private Text newKey = new Text();
        private HashMap<String, Integer> featuremap = new HashMap<String,Integer>();
        private String features = new String();
        private List<String> temp_values = new ArrayList<String>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            Configuration conf = context.getConfiguration();
            features = conf.get("FEATURE2");
            int index1 = 0;
            int index2 = features.indexOf("#");
            //String temp = features.substring(index+1);
            while(index2 != -1){
                String feature = features.substring(index1,index2);
                int index = feature.indexOf("\t");

                featuremap.put(feature.substring(0,index),Integer.parseInt(feature.substring(index+1)));
                index1 = index2 + 1;
                if(index1 < features.length())
                    index2 = features.substring(index1).indexOf("#")+index1;
                else
                    break;
            }
        }

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int wordcount = 0,filecount = 19997;
            String result;
            String results = "";
            //Iterable<Text> temp_values = values;
            //Iterator<Text> tem = values.iterator();
            for(Text info : values) {
                //Text info = tem.next();
                String strInfo = info.toString();
                //result += strInfo1;
                //files++;
                String[] splits = strInfo.split(":");
                wordcount += Integer.parseInt(splits[1]);
                temp_values.add(strInfo);
            }
            //Iterator<Text> it = values.iterator();
            for (String strInfo : temp_values) {
                //Text info = it.next();
                //String strInfo = info.toString();
                String[] splits = strInfo.split(":");

                double tf = (double) Integer.parseInt(splits[1]) / wordcount;

                if(featuremap.containsKey(splits[0])) {
                    int num = featuremap.get(splits[0]);
                    double idf = Math.log((double) filecount / num);
                    double tf_idf = tf * idf;

                    result = splits[0] + ":" + Double.toString(tf_idf);
                    System.out.println(result);
                    results = results + " " + result;
                }
            }
            //System.out.println(results);
            int index = key.toString().indexOf(":");
            String kind = key.toString().substring(index+1);
            newKey.set(kind);
            value.set(results);
            context.write(newKey,value);
        }
    }

    public static class Job2Mapper extends Mapper<Object,Text,Text,Text>{
        private Text word = new Text();
        private FileSplit split;
        private Text one = new Text("1");
        public void map(Object key,Text value,Context context)throws IOException, InterruptedException{
            //StringTokenizer itr = new StringTokenizer(value.toString());
            split = (FileSplit) context.getInputSplit();
            String pathname = split.getPath().getName().toString();

            int index = pathname.indexOf(".txt");
            if (index == -1)
                index = pathname.indexOf(".TXT");
            if(index != -1)
                pathname = pathname.substring(0, index);

            //while (itr.hasMoreTokens()) {
                String temp = value.toString();
                int index1 = temp.indexOf("\t");
                word.set(temp.substring(0,index1)+":"+pathname+":"+temp.substring(index1+1));
                //value.set(temp.substring(index1+1));
                //value.set(pathname);
                //word.set(itr.nextToken() + ":" + pathname);
                context.write(word, one);
            //}
        }
    }

    public static class IDFReducer extends Reducer<Text, Text, Text, Text> {
        private Text value = new Text();
        private Text newKey = new Text();
        private HashMap<String, Integer> featuremap = new HashMap<String,Integer>();
        private String features = new String();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            Configuration conf = context.getConfiguration();
            features = conf.get("FEATURE1");
            int index1 = 0;
            int index2 = features.indexOf("#");
            //String temp = features.substring(index+1);
            //System.out.println(features.substring(0,100));
            while(index2 != -1){
                //System.out.println(index1+"+"+index2);
                String feature = features.substring(index1,index2);
                //System.out.println(feature);
                int index = feature.indexOf("\t");
                //System.out.println(feature.substring(index+1));
                featuremap.put(feature.substring(index+1),1);
                index1 = index2 + 1;
                if(index1 < features.length())
                    index2 = features.substring(index1).indexOf("#")+index1;
                else
                    break;
            }
        }

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int filecount = 0;
            for (Text info : values) {
                filecount++;
            }
            if(featuremap.containsKey(key.toString())) {
                featuremap.put(key.toString(), filecount);
                context.write(key, new Text(Integer.toString(filecount)));
            }
        }


    }
}

