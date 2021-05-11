package com;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName ReduceText
 * @Author mjwang
 * @Date 2021/5/11 10:10
 * @Description ReduceText
 * @Version 1.0
 */
public class ReduceText extends Reducer<Text, LongWritable, Text, LongWritable > {
    @Override
    protected void reduce(Text key, Iterable<LongWritable > values, Context context) throws IOException, InterruptedException {
        int count = 0;

        for (LongWritable  value : values) {//迭代求和

            count += value.get();

        }
        context.write(key, new LongWritable (count));//写到上下文
    }
}
