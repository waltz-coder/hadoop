package com.waltz.maxvalue;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;


public class MaxValue extends Configured implements Tool {

    private static class MaxValueMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private int max_value = Integer.MIN_VALUE;
        private String key_out = "";

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int tmp = 0;
            String[] splits = value.toString().split("\t");
            if (splits.length == 2) {
                tmp = Integer.parseInt(splits[1]);
                if (max_value < tmp) {
                    max_value = tmp;
                    key_out = splits[0];
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(key_out), new LongWritable(max_value));
        }
    }


    private static class MaxValueReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private long max_value = Long.MIN_VALUE;
        private Text key_out;

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long tmp = 0;
            for (LongWritable value : values) {
                tmp = value.get();
                if (max_value < tmp) {
                    max_value = tmp;
                }
            }
            key_out = new Text(key);
        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(key_out, new LongWritable(max_value));
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }
}
