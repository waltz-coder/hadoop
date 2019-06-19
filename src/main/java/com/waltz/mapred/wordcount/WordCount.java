package com.waltz.mapred.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 统计文本文件的单词数，并输出到一个文件中
 */
public class WordCount extends Configured implements Tool {


    private static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final LongWritable ONE = new LongWritable(1);
        private final Text KEY_OUT = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("map begin");
            String[] splits = value.toString().split(" ");
            for (String split : splits) {

                split = split.toLowerCase();
                split = split.replaceAll("[^a-z]+", "");
                if (split.equals(" ")) {
                    continue;
                }
                System.out.println("split\t" + split);
                KEY_OUT.set(split);
                context.write(KEY_OUT, ONE);
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
                count++;
            }
            VALUE_OUT.set(count);
            context.write(key, VALUE_OUT);
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
        return job.waitForCompletion(false) ? 0 : 1;
    }

    public static void main(String[] args) {

        try {
            System.exit(ToolRunner.run(new WordCount(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
