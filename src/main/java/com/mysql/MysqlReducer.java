package com.mysql;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName MysqlReducer
 * @Author mjwang
 * @Date 2021/5/11 17:29
 * @Description MysqlReducer
 * @Version 1.0
 */
public class MysqlReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (LongWritable  value : values) {//迭代求和
            count += value.get();
        }
        context.write(new Text(key), new LongWritable (count));
    }
}
