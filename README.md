# sf-crime-analysis



    1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

    From the data I recorded in StreamingThroughput.csv, increasing maxRatePerPartition seemed to increase the difference between inputRowsPerSecond and processedRowsPerSecond.  This difference is a measure of throughput, and if it gets too large the application may not be keeping up with incoming data and needs to be scaled.  This difference was greatest in the first batch, but lessened as processing continued.  In my testing, I saw this value decrease as batches were processsed for both 'earliest' and 'latest' startingOffsets.  

    Also, when startingOffsets was set to earliest, then the application must process all the existing input data on startup, which accounts for the large difference between inputRowsPerSecond and processedRowsPerSecond in the first batch.  This difference was much smaller when startingOffsets was set to latest.


    2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

    In my testing, setting maxRatePerPartion to 1000, with startingOffsets set to latest, seemed the best setting for equalizing inputRowsPerSecond and processedRowsPerSecond.


Sites:

https://stackoverflow.com/questions/49598732/how-can-i-use-spark-streamingqueryprogress-to-accurately-monitor-performance
https://databricks.com/blog/2017/05/18/taking-apache-sparks-structured-structured-streaming-to-production.html

    3.  What are the top types of crimes in San Fransisco?
    
    Passing Call|35180


    4. What is the crime density by location?

    Premise Address|102791
