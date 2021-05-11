package com.mysql;

import com.MapperText;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;

import java.io.IOException;
import java.sql.Driver;

/**
 * @ClassName MysqlDriver
 * @Author mjwang
 * @Date 2021/5/11 17:25
 * @Description MysqlDriver
 * @Version 1.0
 */
public class MysqlDriver {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir","H:\\hadoop-3.1.2");

        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver","jdbc:mysql://192.168.61.78:3306/hubservice_api_log?useUnicode=true&characterEncoding=UTF-8","chaxun","chaxun");
        Job job = Job.getInstance(conf);
        job.setJarByClass(MysqlDriver.class);
        job.setNumReduceTasks(10);



        job.setMapperClass(MysqlMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(MysqlReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        String [] f1 ={"QueryId","TransCode","TransInfo","QueryAccount","WSStatus","WSDetail","ErrMsg","DataSource","relaDSTrans","retDataNum","RequestAddr","Begintime","Endtime","transkeyword","traceId"};
        DBInputFormat.setInput(job,QueryRecordBean.class,"eds_query_history2","begintime>'2021/01/11' and transcode<>'' " ,"begintime desc",f1);
//        DBOutputFormat.setOutput(job,"film_result",f2);
        Path path = new Path("H:\\workspace4\\MapReduce\\src\\main\\resources\\output2");
        FileOutputFormat.setOutputPath(job, path);//指定输出位置

        FileSystem hdfs = path.getFileSystem(conf);
        if (hdfs.exists(path)) {
            hdfs.delete(path, true);
        }

        System.exit(job.waitForCompletion(true)?0:1);

    }
}
