package com.lendap.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.time.Instant;

public class MatrixMultiply {
	
    public static void main(String[] args) throws Exception {
    	if (args.length != 2) {
            System.err.println("Usage: MatrixMultiply <in_dir> <out_dir>");
            System.exit(2);
        }

        //read configs for the test
        File configFile = new File(args[0]+"/config");
        BufferedReader b = new BufferedReader(new FileReader(configFile));
        String configLine;
        if ((configLine = b.readLine()) == null){
            System.out.println("Could not read the configurations");
            return;
        }

        String[] configs = configLine.split(",");
        if(configs.length != 3){
            System.out.println("Config file content is invalid");
            return;
        }

        //Create a new configuration object to pass the configs to hadoop nodes
        Configuration conf = new Configuration();
        // m and n denotes the dimensions of matrix M.
        // m = number of rows
        // n = number of columns
        conf.set("m", configs[0]);
        conf.set("n", configs[1]);
        // n and p denotes the dimensions of matrix N
        // n = number of rows
        // p = number of columns
        conf.set("p", configs[2]);

        // Setting an output directory
        String outputDir = args[1] + "/" + Instant.now().toEpochMilli();

        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "MatrixMultiply");
        job.setJarByClass(MatrixMultiply.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));
 
        job.waitForCompletion(true);
    }
}