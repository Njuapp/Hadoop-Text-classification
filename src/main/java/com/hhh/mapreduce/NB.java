package com.hhh.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by joker on 17-7-15.
 */
public class NB {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        if(args.length != 3){
            System.err.println("Usage: NB <train_file><test_file><prediction>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Path out = new Path(args[2]);
        FileSystem f = FileSystem.get(conf);
        if (f.exists(out))
            f.delete(out, true);

        Job job = Job.getInstance(conf, "NB");
        job.addCacheFile(new Path(args[0]).toUri());
        job.setJarByClass(NB.class);
        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setMapperClass(NB.NBMapper.class);
        job.setReducerClass(NB.NBReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }

    public static class Instance {
        int label;
        private int feature_num = 10001;
        int[] feature = new int[feature_num];
        public Instance(String[] tokens){
            label = Integer.parseInt(tokens[0]);
            for(int i = 1; i < tokens.length;i++){
                int index = tokens[i].indexOf(":");
                int word_idx = Integer.parseInt(tokens[i].substring(0, index));
                int word_val = Integer.parseInt(tokens[i].substring(index + 1));
                feature[word_idx] = word_val;
            }
        }

    }
    public static class NBMapper extends Mapper<LongWritable, Text, Text, Text>{
        ArrayList<Instance> train = new ArrayList<Instance>(19997);
        Map<String,Integer> classWordsNum = new TreeMap<String,Integer>();
        Map<String,Integer> classWordsProb = new TreeMap<String, Integer>();
        private int totalTrainWordCount = 0;

        public void setup(Mapper.Context context){
            try{
                URI[] files = context.getCacheFiles();
                String line;
                String[] tokens;
                if(files != null && files.length > 0){
                    Path path2 = new Path(files[0]);
                    FileSystem hdfs = FileSystem.get(context.getConfiguration());
                    FSDataInputStream dis = hdfs.open(path2);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(dis));
                   // BufferedReader reader = new BufferedReader(new FileReader(files[0].toString()));
                    try{

                        while((line = reader.readLine())!= null){
                            tokens = line.split("\\s+");
                            //Instance temp = new Instance(tokens);
                            //train.add(new Instance(tokens));
                            String k = tokens[0];
                            int count = Integer.parseInt(tokens[1]);
                            if (classWordsProb.containsKey(k)) {
                                int val = classWordsProb.get(k);
                                classWordsProb.put(k, val + count);
                            } else
                                classWordsProb.put(k, count);
                            /*int label = temp.label;
                            if(classWordsNum.containsKey(Integer.toString(label))){
                                int n = classWordsNum.get(Integer.toString(label));
                                classWordsNum.put(Integer.toString(label),n+1);
                            }
                            else
                                classWordsNum.put(Integer.toString(label),1);*/

                        }

                    }
                    finally {
                        reader.close();
                    }
                }

            }catch(IOException e){
                System.err.println("Reading cache file error!"+ e);
            }
            for(Map.Entry<String,Integer> entry: classWordsProb.entrySet()){
                String k = entry.getKey();
                int index = k.indexOf("_");
                int val = entry.getValue();
                String label = k.substring(0,index);
                if(classWordsNum.containsKey(label)){
                    int n = classWordsNum.get(label);
                    classWordsNum.put(label,n+val);
                }
                else
                    classWordsNum.put(label,val);
            }
            for(Map.Entry<String,Integer> entry: classWordsNum.entrySet()){
                int val = entry.getValue();
                totalTrainWordCount += val;
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Instance test = new Instance(value.toString().split("\\s+"));
            int[] trainClassWordCount = new int[20];


           /* for(int i = 0;i<20;i++){
                if(!classWordsNum.containsKey(Integer.toString(i)))
                    classWordsNum.put(Integer.toString(i),1);
            }*/

            /*for(Instance tr: train){
                int label = tr.label;
                for(int i = 1;i<tr.feature_num;i++){
                    if(tr.feature[i]!=0) {
                        String k = label + "_" + i;
                        if (classWordsProb.containsKey(k)) {
                            int val = classWordsProb.get(k);
                            classWordsProb.put(k, val + tr.feature[i]);
                        } else
                            classWordsProb.put(k, tr.feature[i]);
                    }
                }
                if(classWordsNum.containsKey(Integer.toString(label))){
                    int n = classWordsNum.get(Integer.toString(label));
                    classWordsNum.put(Integer.toString(label),n+1);
                }
                else
                    classWordsNum.put(Integer.toString(label),1);
            }*/


            for(Map.Entry<String,Integer> entry: classWordsProb.entrySet()){
                String k = entry.getKey();
                int index = k.indexOf("_");
                int cate = Integer.parseInt(k.substring(0,index));
                trainClassWordCount[cate]++;
            }
            /*for(int i = 0;i<20;i++){
                if(trainClassWordCount[i] == 0)
                    trainClassWordCount[i] = 20000;
            }*/
            int prediction = -1;
            BigDecimal testCateProb = new BigDecimal(0.0);
            String results = "";
            String result = "";
            for(int cate = 0; cate < 19; cate++){
                BigDecimal testOneCateProb = new BigDecimal(1.0);
                int testWordNumInClass;
                BigDecimal condProb = new BigDecimal(1.0);
                for (int i = 0; i < test.feature_num; i++) {
                    if (test.feature[i] != 0) {
                        String k = cate + "_" + i;
                        if (classWordsProb.containsKey(k)) {
                            testWordNumInClass = classWordsProb.get(k);
                        } else
                            testWordNumInClass = 0;
                        BigDecimal testWordNumInClassBD = new BigDecimal(testWordNumInClass);
                        //condProb =  ((double)testWordNumInClass + 0.0001) / (classWordsNum.get(Integer.toString(cate)) + trainClassWordCount[cate]);
                        condProb = (testWordNumInClassBD.add(new BigDecimal(1))).divide(new BigDecimal(classWordsNum.get(Integer.toString(cate))).add(new BigDecimal(trainClassWordCount[cate])),10,BigDecimal.ROUND_CEILING);
                    }

                    for(int j=0;j<test.feature[i];j++)
                        testOneCateProb = testOneCateProb.multiply(condProb);
                    //testOneCateProb = testOneCateProb.abs();
                }

                BigDecimal classWordsNumBD = new BigDecimal(classWordsNum.get(Integer.toString(cate)));
                BigDecimal totalTrainWordCountBD = new BigDecimal(totalTrainWordCount);
                testOneCateProb = testOneCateProb.multiply( classWordsNumBD.divide(totalTrainWordCountBD,10,BigDecimal.ROUND_CEILING));
                //if(testOneCateProb.compareTo(BigDecimal.ZERO) != 1)
                  //  testOneCateProb = testOneCateProb.abs();
                result = cate + ":" + testOneCateProb;
                if(testOneCateProb .compareTo(testCateProb) == 1){
                    prediction = cate;
                    testCateProb = testOneCateProb;
                }
                results += " " + result;
            }

            if(prediction != test.label){
                context.write(new Text("wrong"), new Text("1"));
            }
            else{
                context.write(new Text(("right")), new Text("1"));
            }
        }

    }

    public static class NBReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Integer sum = 0;
            for(Text val: values) {
                sum++;
                //context.write(key, val);
            }
            context.write(key, new Text(sum.toString()));
        }
    }
}