package com.mysql;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName MapperAndReducer
 * @Author mjwang
 * @Date 2021/5/11 17:29
 * @Description MapperAndReducer
 * @Version 1.0
 */
public class MysqlMapper extends Mapper<LongWritable, QueryRecordBean, Text, LongWritable> {


    @Override
    protected void map(LongWritable key, QueryRecordBean value, Context context) throws IOException, InterruptedException {
        context.write(new Text(value.getTransCode()),new LongWritable(1));
    }
}



