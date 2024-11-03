# Remove folders of the previous run
hdfs dfs -rm -r example_data
hdfs dfs -rm -r example_out

# Put input data collection into hdfs
hdfs dfs -put example_data


# Run application
hadoop jar ex.jar it.polito.bigdata.hadoop.DriverBigData 2 input  output_job1




