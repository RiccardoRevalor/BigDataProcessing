package it.polito.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * MapReduce program
 */
public class DriverBigData extends Configured 
implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    //2 JOBS ARE NEEDED
    //1st JOB: calculate user's normalized rating and save it in a temporary folder
    //2nd JOB: calculate the average rating of each product and save it in the output folder

    Path inputPath;
    Path outputDir;
    int numberOfReducers;
	int exitCode;  
	
	// Parse the parameters
	// Number of instances of the reducer class 
    numberOfReducers = Integer.parseInt(args[0]);
    // Folder containing the input data
    inputPath = new Path(args[1]);
    // Output folder
    outputDir = new Path(args[2]);
    
    Configuration conf = this.getConf();

    // Define a new job
    Job job = Job.getInstance(conf); 

    // Assign a name to the job
    job.setJobName("Lab4 - Job1");
    
    // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
    FileInputFormat.addInputPath(job, inputPath);
    
    // Set path of the output folder for this job
    FileOutputFormat.setOutputPath(job, outputDir);
    
    // Specify the class of the Driver for this job
    job.setJarByClass(DriverBigData.class);
    
    
    // Set job input format
    job.setInputFormatClass(TextInputFormat.class);

    // Set job output format
    job.setOutputFormatClass(TextOutputFormat.class);
       
    // Set map class
    job.setMapperClass(MapperBigData.class);
    
    // Set map output key and value classes
    job.setMapOutputKeyClass(Text.class); //key=userId (Text)
    job.setMapOutputValueClass(ProductRating.class); //value=productRating
    
    // Set reduce class
    job.setReducerClass(ReducerBigData.class);
        
    // Set reduce output key and value classes
    job.setOutputKeyClass(Text.class); //key=productId (Text) for each user's review
    job.setOutputValueClass(DoubleWritable.class); //value=averageRating (DoubleWritable)

    // Set number of reducers
    job.setNumReduceTasks(numberOfReducers);
    
    
    // Execute the job and wait for completion
    if (job.waitForCompletion(true)==true) {

      System.out.println("Job1 completed succesfully");

      //here goes the second job
      Path outputDir2;
    
      // Parse the parameters
      // Number of instances of the reducer class 
      numberOfReducers = Integer.parseInt(args[3]);
      // Output folder
      outputDir2 = new Path(args[4]);
      
      conf = this.getConf();

      // Define a new job
      Job job2 = Job.getInstance(conf); 

      // Assign a name to the job
      job2.setJobName("Lab4 - Job2");
      
      // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
      FileInputFormat.addInputPath(job2, outputDir);
      
      // Set path of the output folder for this job
      FileOutputFormat.setOutputPath(job2, outputDir2);
      
      // Specify the class of the Driver for this job
      job2.setJarByClass(DriverBigData.class);
      
      
      // Set job input format
      job2.setInputFormatClass(KeyValueTextInputFormat.class);

      // Set job output format
      job2.setOutputFormatClass(TextOutputFormat.class);
        
      // Set map class
      job2.setMapperClass(MapperBigData2.class);
      
      // Set map output key and value classes
      job2.setMapOutputKeyClass(Text.class); //key=userId (Text)
      job2.setMapOutputValueClass(DoubleWritable.class); //value=productRating
      
      // Set reduce class
      job2.setReducerClass(ReducerBigData2.class);
          
      // Set reduce output key and value classes
      job2.setOutputKeyClass(Text.class); //key=productId (Text) for each user's review
      job2.setOutputValueClass(DoubleWritable.class); //value=averageRating (DoubleWritable)

      // Set number of reducers
      job2.setNumReduceTasks(numberOfReducers);
      
      
      // Execute the job and wait for completion
      if (job2.waitForCompletion(true)==true) {

        System.out.println("Job2 completed succesfully");

        
        exitCode=0;
      }else {

        System.out.println("Job2 failed");
        exitCode=1;
      }
    }else {

      System.out.println("Job1 failed");
      exitCode=1;
    }
    	
    return exitCode;
  }
  

  /** Main of the driver
   */
  
  public static void main(String args[]) throws Exception {
	// Exploit the ToolRunner class to "configure" and run the Hadoop application
    int res = ToolRunner.run(new Configuration(), 
    		new DriverBigData(), args);

    System.exit(res);
  }
}