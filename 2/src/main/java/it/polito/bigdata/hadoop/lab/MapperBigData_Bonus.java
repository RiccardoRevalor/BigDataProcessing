package it.polito.bigdata.hadoop.lab2;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper
 */
class MapperBigData extends
		Mapper<Text, // Input key type
				Text, // Input value type
				Text, // Output key type
				Text> {// Output value type

	String search;

	protected void setup(Context context) {
		search = context.getConfiguration().get("search").toString();
	}

	protected void map(Text key, 	// Input key type
			Text value, 			// Input value type
			Context context) throws IOException, InterruptedException {

		//BI-GRAMS
        //Split the bigram
        String[] words = key.toString().split(" ");
        //like can be either the first or the second word of the selected 2-grams
        if (words[0].compareTo(search) == 0 || words[1].compareTo(search) == 0) {
            //update counter
            context.getCounter("COUNTERS", "SELECTED_WORDS").increment(1);
            context.write(key, value);
        } else {
            //update counter
            context.getCounter("COUNTERS", "DISCARDED_WORDS").increment(1);
        }
	}

}
