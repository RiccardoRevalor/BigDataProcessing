package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver class.
 */
public class DriverBigData extends Configured implements Tool {

	public static enum COUNTERS {
		SELECTED_WORDS, 
		DISCARDED_WORDS
	}

	@Override
	public int run(String[] args) throws Exception {

		Path inputPath;
		Path outputDir;
		int exitCode;

		// Parse the parameters
		inputPath = new Path(args[0]);
		outputDir = new Path(args[1]);

		Configuration conf = this.getConf();

    	conf.setBoolean("hadoop.native.lib", false);

		// Get the prefix from the argument list and set it in the configuration.
		conf.set("prefix", args[2]);

		// Define a new job
		Job job = Job.getInstance(conf);

		// Assign a name to the job
		job.setJobName("Lab2_task1_filtering");

		// Set path of the input file/folder (if it is a folder, the job reads all the
		// files in the specified folder) for this job
		FileInputFormat.addInputPath(job, inputPath);

		// Set path of the output folder for this job
		FileOutputFormat.setOutputPath(job, outputDir);

		// Specify the class of the Driver for this job
		job.setJarByClass(DriverBigData.class);

		// Set input format
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// Set job output format
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set map class
		job.setMapperClass(MapperBigData.class);

		// Set map output key and value classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Set number of reducers
		job.setNumReduceTasks(0);

		// Execute the job and wait for completion
		if (job.waitForCompletion(true) == true) {
			exitCode = 0;
			Counter selectedWords = job.getCounters().findCounter(COUNTERS.SELECTED_WORDS);
			Counter discardedWords = job.getCounters().findCounter(COUNTERS.DISCARDED_WORDS);
			
			System.out.println("Selected: "+selectedWords.getValue());
			System.out.println("Discarded: "+discardedWords.getValue());
		}
		else
			exitCode = 1;

		return exitCode;
	}

	/**
	 * Main of the driver
	 */

	public static void main(String args[]) throws Exception {
		// Exploit the ToolRunner class to "configure" and run the Hadoop application
		int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

		System.exit(res);
	}
}