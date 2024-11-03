package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    ProductRating> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            //filter the header of the csv file

            String[] args = value.toString().split(",");
            String pid = args[1];
            if (pid.toLowerCase().equals("productid")) return; //do not send data 

            String uid = args[2];
            String scoreStr = args[6];
            //check if the score is a number
            if (!scoreStr.matches("[-+]?\\d*\\.?\\d+")) return; //do not send data
            Double score = Double.parseDouble(scoreStr);

            context.write(new Text(uid), new ProductRating(pid, score));


    }
}
