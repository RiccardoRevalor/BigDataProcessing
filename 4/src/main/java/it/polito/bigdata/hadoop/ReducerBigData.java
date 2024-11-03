package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                ProductRating,    // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<ProductRating> values, // Input value type
        Context context) throws IOException, InterruptedException {


        //goal of the first reducer: normalize rating according to the users scores and update the rating of teh product
        //then, write in thex context: (productId rating) (users is not important anymore for the computation)
        
        //since two iterations are needed, we have to store the ratings in a local data structure
        HashMap<String, Double> productsRatings = new HashMap<String, Double>();

    
        double avgRating = 0;
        double sum = 0, count = 0;

        //compute the average rating of the user
        for (ProductRating p: values){
            sum += p.getRating();
            count++;

            productsRatings.put(p.getProductId(), p.getRating());
        }

        avgRating = sum / count;

        for (Entry<String, Double> entry : productsRatings.entrySet()) {
            //update the rating subtracting the average rating of the user
            //send the new rating to the context

            context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue() - avgRating));

        }
    }
}
