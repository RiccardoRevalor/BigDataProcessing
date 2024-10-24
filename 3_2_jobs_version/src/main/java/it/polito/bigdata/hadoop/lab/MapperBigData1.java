package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		//MAPPER 1: compute the number of occrrences of each pair of products
            //items[0] -> customer id, don't care
            //items[>=1] -> product id
            //ex: A09539661HB8JHRFVRDC,B002R8UANK,B002R8J7YS,B002R8SLUY

            String[] items = value.toString().split(",");
            int l = items.length;

            for (int i =1; i < l; i++){
                for (int j = 1; j < l; j++){
                    if (i == j) continue;
                    if (!items[i].equals(items[j])){
                        context.write(new Text(items[i] + "," + items[j]), new IntWritable(1));
                    }
                }
            }
    }
}
