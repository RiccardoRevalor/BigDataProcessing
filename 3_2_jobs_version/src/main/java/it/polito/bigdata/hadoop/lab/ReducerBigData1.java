package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                IntWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		//REDUCER 1: compute the number of occrrences of each pair of products
        //key: the pair (ex A,B)
        //values: the number of occurrences of the pairù
        //sum=total occurrences of the pair

        int sum = 0;
        for (IntWritable value : values){
            sum += value.get();
        }

        context.write(key, new IntWritable(sum));
    	
    }
}
