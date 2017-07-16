package com.hhh.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by joker on 17-7-15.
 */
public class KNN {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        if(args.length != 3){
            System.err.println("Usage: KNN <train_file><test_file><prediction>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KNN");
        job.addCacheFile(new Path(args[0]).toUri());
        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setMapperClass(KNNMapper.class);
        job.setReducerClass(KNNReducer.class);
        job.waitForCompletion(true);
    }

    public static class Instance {
        int label;
        double[] feature = new double[5000];
        public Instance(String[] tokens){
            label = Integer.parseInt(tokens[0]);
            for(int i = 1; i < tokens.length;i++){
                int index = tokens[i].indexOf(":");
                int word_idx = Integer.parseInt(tokens[i].substring(0, index));
                double word_val = Double.parseDouble(tokens[i].substring(index + 1));
                feature[word_idx] = word_val;
            }
        }
        public static double distance (Instance a, Instance b){
            double norm1 = 0.0, norm2 = 0.0, prod = 0.0;
            for(int i = 0; i < 5000; i++){
                norm1 += a.feature[i] * a.feature[i];
                norm2 += b.feature[i] * b.feature[i];
                prod += a.feature[i] * b.feature[i];
            }
            return prod / (Math.sqrt(norm1 * norm2));
        }
    }
    public static class KNNMapper extends Mapper<LongWritable, Text, Text, Text>{
        ArrayList<Instance> train = new ArrayList<Instance>(19997);
        public void setup(Mapper.Context context){
            try{
                URI[] files = context.getCacheFiles();
                String line;
                String[] tokens;
                if(files != null && files.length > 0){
                    BufferedReader reader = new BufferedReader(new FileReader(files[0].toString()));
                    try{
                        while((line = reader.readLine())!= null){
                            tokens = line.split("/s");
                            //TODO:parse the train file into instances
                            train.add(new Instance(tokens));
                        }
                    }
                    finally {
                        reader.close();
                    }
                }

            }catch(IOException e){
                System.err.println("Reading cache file error!"+ e);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Instance test = new Instance(value.toString().split("/s"));
            double[] weight = new double[20];
            for(Instance tr: train){
                double cos = Instance.distance(test, tr);
                weight[tr.label] += cos;
            }
            int pred = -1;
            double max = Double.MIN_VALUE;
            for(int i = 0; i < weight.length; i++){
                if ( weight[i] > max){
                    max = weight[i];
                    pred = i;
                }
            }
            if(pred != test.label){
                context.write(new Text("wrong"), new Text("1"));
            }
            else{
                context.write(new Text(("right")), new Text("1"));
            }
        }

    }

    public static class KNNReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Integer sum = 0;
            for(Text val: values) {
                sum++;
            }
            context.write(key, new Text(sum.toString()));
        }
    }
}