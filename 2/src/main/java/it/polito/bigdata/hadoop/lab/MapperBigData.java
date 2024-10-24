package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
    protected String prefix;

    protected void setup(Context context){
        //get the prefix from the configuration
        prefix = context.getConfiguration().get("prefix");
    }
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		//input:
            //key: string, it's a word
            //value: numeric string, it's the number of occurrences of the word

            if (key.toString().startsWith(prefix)) {
                //output:
                //key: string, it's a word
                //value: numeric string, it's the number of occurrences of the word
                context.write(key, value);
                //increment the counter
                context.getCounter(DriverBigData.COUNTERS.SELECTED_WORDS).increment(1);
            } else {
                //do not send anything on the net
                //just increment the counter
                context.getCounter(DriverBigData.COUNTERS.DISCARDED_WORDS).increment(1);
            }
    }
}
