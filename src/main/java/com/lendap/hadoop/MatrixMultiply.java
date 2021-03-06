package com.lendap.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.time.Instant;
import java.util.HashMap;

public class MatrixMultiply {

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int m = Integer.parseInt(conf.get("m"));
            int p = Integer.parseInt(conf.get("p"));
            String line = value.toString();
            // (M, i, j, Mij);
            String[] indicesAndValue = line.split(",");
            Text outputKey = new Text();
            Text outputValue = new Text();
            if (indicesAndValue[0].equals("M")) {
                for (int k = 0; k < p; k++) {
                    outputKey.set(indicesAndValue[1] + "," + k);
                    outputValue.set(indicesAndValue[0] + "," + indicesAndValue[2]
                            + "," + indicesAndValue[3]);
                    context.write(outputKey, outputValue);
                }
            } else if (indicesAndValue[0].equals("N")) {
                for (int i = 0; i < m; i++) {
                    outputKey.set(i + "," + indicesAndValue[2]);
                    outputValue.set(indicesAndValue[0] + "," + indicesAndValue[1]
                            + "," + indicesAndValue[3]);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    public static class Reduce
            extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String[] value;
            //key=(i,k),
            //Values = [(M/N,j,V/W),..]
            HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
            HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
            for (Text val : values) {
                value = val.toString().split(",");
                if (value[0].equals("M")) {
                    hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                } else {
                    hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                }
            }
            int n = Integer.parseInt(context.getConfiguration().get("n"));
            float result = 0.0f;
            float m_ij;
            float n_jk;
            for (int j = 0; j < n; j++) {
                m_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
                n_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
                result += m_ij * n_jk;
            }
            if (result != 0.0f) {
                context.write(null,
                        new Text(key.toString() + "," + Float.toString(result)));
            }
        }
    }
	
    public static void main(String[] args) throws Exception {
    	if (args.length != 2) {
            System.err.println("Usage: MatrixMultiply <in_dir> <out_dir>");
            System.exit(2);
        }


        //Create a new configuration object to pass the configs to hadoop nodes
        Configuration conf = new Configuration();

        Path path = new Path(args[0]+"/config");
        FileSystem fs = path.getFileSystem(conf);
        BufferedReader b = new BufferedReader(new InputStreamReader(fs.open(path)));

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