package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.commons.beanutils.converters.IntegerArrayConverter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    WordCountWritable> {// Output value type

    protected TopKVector<WordCountWritable> localTopK;

    protected void setup(Context context) {
        // Create a new LOCAL TopKVector
        localTopK = new TopKVector<WordCountWritable>(100);
    }
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		//MAPPER 2: perform local top-k among the elements

            // key = current pair of products
            // value = num. of occurrences of the current pair

            WordCountWritable n = new WordCountWritable(key.toString(), Integer.parseInt(value.toString()));

            //the hypotethical insert and sorting of the new elements is managed by the TopKVector class
            //So, I try to insert it in the local top-k vector

            localTopK.updateWithNewElement(n);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException{
        //emit all the values in the local top-k vector
        for (WordCountWritable wc: localTopK.getLocalTopK()){
            context.write(NullWritable.get(), new WordCountWritable(wc));
        }
    }
}
