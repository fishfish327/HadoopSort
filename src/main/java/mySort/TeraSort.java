package mySort;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TeraSort extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(TeraSort.class);



    private static void usage() throws IOException {
        System.err.println("Usage: terasort [-Dproperty=value] <in> <out>");
        System.err.println("TeraSort configurations are:");
        for (TeraSortConfigKeys teraSortConfigKeys : TeraSortConfigKeys.values()) {
            System.err.println(teraSortConfigKeys.toString());
        }
        System.err.println("If you want to store the output data as " +
                "erasure code striping file, just make sure that the parent dir " +
                "of <out> has erasure code policy set");
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            usage();
            return 2;
        }
        long startTime = System.currentTimeMillis();
        LOG.info("starting");
        Job job = Job.getInstance(getConf());
        Path inputDir = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        job.setJobName("TeraSort");
        job.setJarByClass(TeraSort.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Setup MapReduce
        job.setMapperClass(MySortMapper.class);
        job.setReducerClass(MySortReducer.class);

        // Setup Partitioner
        Path partitionFile = new Path(inputDir, "partition");
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);
        job.setPartitionerClass(TotalOrderPartitioner.class);

        // Input
        FileInputFormat.addInputPath(job, inputDir);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job
        Integer numReduceTasks = 4;
        job.setNumReduceTasks(numReduceTasks);
        double pcnt = 10.0;
        int numSamples = numReduceTasks;
        int maxSplits = numReduceTasks - 1;
        if (0 >= maxSplits)
            maxSplits = Integer.MAX_VALUE;

        InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(pcnt, numSamples,
                maxSplits);

        InputSampler.writePartitionFile(job, sampler);


        int ret = job.waitForCompletion(true) ? 0 : 1;
        LOG.info("done");
        long endTime = System.currentTimeMillis();
        System.out.println("Time taken: " + (endTime - startTime));
        return ret;
    }

    public static Configuration setCompressConfig(){
        Configuration conf = new Configuration();
        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.map.output.compression.codec", GzipCodec.class, CompressionCodec.class);

        return conf;
    }
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(setCompressConfig(), new TeraSort(), args);
        System.exit(res);
    }

}