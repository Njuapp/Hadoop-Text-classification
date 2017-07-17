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
import java.util.Iterator;
import java.util.Map;

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
        job.setJarByClass(KNN.class);
        job.addCacheFile(new Path(args[0]).toUri());
        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setMapperClass(KNN.KNNMapper.class);
        job.setReducerClass(KNN.KNNReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }

    public static class Instance {
        int label;
        HashMap<Integer, Double> feature = new HashMap<Integer, Double>();
        public Instance(String[] tokens){
            label = Integer.parseInt(tokens[0]);
            HashMap<Integer, Double> temp = new HashMap<Integer, Double>();
            double norm = 0.0;
            for(int i = 1; i < tokens.length;i++){
                int index = tokens[i].indexOf(":");
                int word_idx = Integer.parseInt(tokens[i].substring(0, index));
                double word_val = Double.parseDouble(tokens[i].substring(index + 1));
                temp.put(word_idx, word_val);
                norm += word_val * word_val;
            }
            norm = Math.sqrt(norm);
            Iterator<Map.Entry<Integer, Double>> it = temp.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry<Integer, Double> entry = it.next();
                Double val = entry.getValue();
                Integer idx = entry.getKey();
                feature.put(idx, val / norm);
            }
        }
        public static double distance (Instance a, Instance b){
            double prod = 0.0;
            Iterator<Map.Entry<Integer, Double>> it = a.feature.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Integer, Double> entry = it.next();
                Integer word_idx = entry.getKey();
                if (b.feature.containsKey(word_idx)) {
                    prod += entry.getValue() * b.feature.get(word_idx);
                }
            }
            return prod ;
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
                            tokens = line.split("\\s+");
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
            Instance test = new Instance(value.toString().split("\\s+"));
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