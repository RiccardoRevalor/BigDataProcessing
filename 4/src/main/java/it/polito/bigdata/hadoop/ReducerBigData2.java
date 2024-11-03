package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.stream.*;
import java.util.stream.StreamSupport;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                DoubleWritable,    // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<DoubleWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {


        //goal of the second reducer: avg rating of the product
        Stream<DoubleWritable> stream = StreamSupport.stream(values.spliterator(), false);
        double avg = stream.mapToDouble(d -> new Double(d.get())).average().getAsDouble();

        context.write(new Text(key), new DoubleWritable(avg));

    }
}
