package com;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName MapperText
 * @Author mjwang
 * @Date 2021/5/11 10:04
 * @Description MapperText
 * @Version 1.0
 */
public class MapperText extends Mapper<LongWritable , Text, Text, LongWritable> {


    @Override
    protected void map(LongWritable  key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] words = line.split(" ");//按照空格切割一行数据得到单词数组

        for (String word : words) {
            context.write(new Text(word), new LongWritable(1));//将每一个单词计数为1写入上下文
        }
    }
}
