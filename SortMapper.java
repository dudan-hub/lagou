package com.lagou.cesi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<Object, Text, IntWritable,IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        final String line = value.toString();
        final String[] str = line.split("\t");
        for (String s : str) {
            context.write(new IntWritable(Integer.parseInt(s)),new IntWritable(1));
        }
    }
}
