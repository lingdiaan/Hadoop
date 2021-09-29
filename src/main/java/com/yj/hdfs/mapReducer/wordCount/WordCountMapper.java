package com.yj.hdfs.mapReducer.wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// 1.x Mapper负责资源调度和计算
// 2.x 只负责调度

/**
 * KEYIN,map阶段输入的key类型，LongWritable
 * VALUEIN,map阶段输入的value类型，Text
 * KEYOUT,最终输出key类型，Text
 * VALUEOUT，map阶段输出的value类型，intWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outKey = new Text();
    private IntWritable outValue = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();
        //2.切割
        String[] words = line.trim().split(" ");
        //3.循环写出
        for (String word : words) {
            outKey.set(word);
            context.write(outKey, outValue);
        }
    }
}
