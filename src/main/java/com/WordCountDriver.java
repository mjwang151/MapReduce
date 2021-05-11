package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;


/**
 * @ClassName WordCountDriver
 * @Author mjwang
 * @Date 2021/5/11 10:02
 * @Description WordCountDriver
 * @Version 1.0
 */
public class WordCountDriver {
    final static Logger log = Logger.getLogger(WordCountDriver. class );

    public static void main(String[] args){
        log.info("============");
        System.setProperty("hadoop.home.dir","H:\\hadoop-3.1.2");
        try {
            //指定配置
            Configuration conf = new Configuration();//设定配置
            Job job = Job.getInstance(conf);
            //指定类
            job.setJarByClass(WordCountDriver.class);//指定主类
            job.setMapperClass(MapperText.class);//指定Mapper
            job.setReducerClass(ReduceText.class);//指定Reduce
            //指定数据类型
            job.setMapOutputKeyClass(Text.class);//指定map阶段的输出key数据类型
            job.setMapOutputValueClass(LongWritable.class);//指定map阶段的输出value数据类型
            job.setOutputKeyClass(Text.class);//指定reduce阶段的输出key数据类型
            job.setOutputValueClass(LongWritable.class);//指定reduce阶段的输出value数据类型
            //指定位置
            FileInputFormat.setInputPaths(job, "H:\\workspace4\\MapReduce\\src\\main\\resources\\input");//指定数据源
            FileOutputFormat.setOutputPath(job, new Path("H:\\workspace4\\MapReduce\\src\\main\\resources\\output"));//指定输出位置
            //设置reduceTask
            job.setNumReduceTasks(2);//表示两个reduceTask,最终输出两个文件,默认对key使用hashcode取余将数据分区
            //提交
            boolean status = job.waitForCompletion(true);//job.submit(); 前者是监控状态提交,后者是直接提交
            //退出
            System.exit(status ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
