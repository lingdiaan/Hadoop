package com.yj.hdfs.mapReducer.wordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.checkerframework.checker.units.qual.C;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job
        Configuration configuration = new Configuration();
        Job instance = Job.getInstance(configuration);
        //2.设置jar包路径
        instance.setJarByClass(WordCountDriver.class);
        //3.关联mapper和reducer
        instance.setReducerClass(WordCountReducer.class);
        instance.setMapperClass(WordCountMapper.class);
        //4.设置map输出的kv类型
        instance.setMapOutputKeyClass(Text.class);
        instance.setMapOutputValueClass(IntWritable.class);

        //5.设置最终输出的kv类型
        instance.setOutputKeyClass(Text.class);
        instance.setOutputValueClass(IntWritable.class);
        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(instance, new Path("F:\\hadoop\\inputWord.txt"));
        FileOutputFormat.setOutputPath(instance, new Path("F\\hadoop\\output1"));
        //7.提交job
        boolean b = instance.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
