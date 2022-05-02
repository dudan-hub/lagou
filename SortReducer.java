package com.lagou.cesi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private static IntWritable num = new IntWritable(1);

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //这里的key就是mapper中转为Integer的每行文本数字
        for (IntWritable value : values) {
            context.write(num,key);
        }
        num = new IntWritable(num.get()+1);
    }
}
