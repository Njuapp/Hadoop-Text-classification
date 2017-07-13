package com.hhh.mapreduce;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

/**
 * Created by nicbh on 2017/7/12.
 */
public class preprocess {
    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("Usage:preprocess <isHDFS><map><input><output>, isHDFS is 0 or 1");
            System.exit(2);
        }
        try {
            HashMap<String, String> classMap = new HashMap<>();
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.getLocal(conf);
            if (args[0].equals("1")) {
                fs = FileSystem.get(conf);
            }
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
//            }else {
//                HashMap<String, String> classMap = new HashMap<>();
//                File mapFile = new File(args[1]);
//                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(mapFile)));
//                String lineTxt;
//                while ((lineTxt = bufferedReader.readLine()) != null) {
//                    String[] txt = lineTxt.split("\\s+");
//                    classMap.put(txt[0], txt[1]);
//                }
//
//                String input = args[2];
//                String output = args[3];
//                File outputDir = new File(output);
//                if (!outputDir.exists()) {
//                    outputDir.mkdirs();
//                }
//                File file = new File(input);
//                File[] classFile = file.listFiles();
//                int fileCount = 0;
//                for (File f : classFile) {
//                    String classname = f.getName();
//                    if (!classname.equals(".DS_Store")) {
//                        String classNum = classMap.get(classname);
//                        File[] textFile = f.listFiles();
//                        System.out.println(classname + "\t" + textFile.length);
//                        for (File textfile : textFile) {
//                            String filename = textfile.getName();
//                            if (!filename.equals(".DS_Store")) {
//                                Reader reader = new FileReader(textfile);
//                                EnglishAnalyzer analyzer = new EnglishAnalyzer(Version.LUCENE_46);
//                                TokenStream tokenStream = analyzer.tokenStream("contents", reader);
//                                TokenStream ts = analyzer.tokenStream("fieldName", reader);
//                                ts.reset();
//                                FileWriter writer = new FileWriter(new File(output + "/" + Integer.toString(fileCount)));
//                                while (ts.incrementToken()) {
//                                    CharTermAttribute ca = ts.getAttribute(CharTermAttribute.class);
//                                    writer.write(ca.toString() + "\t" + classNum + "\n");
//                                }
//                                writer.close();
//                                fileCount++;
//                            }
//                        }
//                    }
//                }
//                System.out.println(fileCount);
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}