package com.waltz.counter;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 统计文本文件的单词数，并输出到一个文件中
 * <p>
 * 利用counter统计map输入的单词数和
 * reduce输出的单词数
 */
public class WordCountWithCounter extends Configured implements Tool {

    public static void main(String[] args) {

        try {
            System.exit(ToolRunner.run(new WordCountWithCounter(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "WordCount");
        job.setOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setJarByClass(getClass());
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(conf);

        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        /**
         * 打印counter信息
         */
        boolean status = job.waitForCompletion(false);

        //获取作业的所有counters
        Counters counters = job.getCounters();

        CounterGroup counterGroup = counters.getGroup("waltz");
        for (Counter counter : counterGroup) {
            System.out.println(counter.getDisplayName() + "=" + counter.getValue());
        }

        return status ? 0 : 1;
    }

    private static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final LongWritable ONE = new LongWritable(1);
        private final Text KEY_OUT = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("map begin");
            String[] splits = value.toString().split("\\u0020");
            for (String split : splits) {
                System.out.println("split:\t" + split);
                KEY_OUT.set(split);
                context.write(KEY_OUT, ONE);
                context.getCounter("waltz", "map input text").increment(1l);
            }
        }
    }

    private static class WordCountReducer extends Reducer<Text, LongWritable, Text, IntWritable> {
        private final IntWritable VALUE_OUT = new IntWritable();
        int count = 0;

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            count = 0;
            for (LongWritable value : values) {
                count += value.get();
            }
            VALUE_OUT.set(count);
            context.write(key, VALUE_OUT);
            context.getCounter("waltz", "reduce output ").increment(1l);
        }
    }
}
