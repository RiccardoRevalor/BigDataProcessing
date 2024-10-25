package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                NullWritable,           // Input key type
                WordCountWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<WordCountWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		//REDUCER 2: JUST 1 REDUCER
        //It implements TOP-K Algorithm globally

        //initialize the GLOBAL top-k vector
        TopKVector<WordCountWritable> globalVect = new TopKVector<>(100);

        //the insert is managed automatically by topkvector library
        values.forEach(v -> globalVect.updateWithNewElement(new WordCountWritable(v)));
        

        //after all the insertions emit the last version of the GLOBAL top-k vector in the context

        for (WordCountWritable v: globalVect.getLocalTopK()){
            context.write(new Text(v.getWord()), new IntWritable(v.getCount()));
        }
    	
    }
}
