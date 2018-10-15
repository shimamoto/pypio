# pypio

memo
- Specify master and deploy mode in the `livy.conf` file.
- To submit the SparkPi application to the Livy server, use the a `POST /batches` request.
```
curl -k -v -H 'Content-Type: application/json' -X POST -d '{ "file":"file:///spark/examples/jars/spark-examples_2.11-2.2.2.jar", "className":"org.apache.spark.examples.SparkPi" }' "http://localhost:8998/batches"
```
Note: the POST request does not upload local jars to the cluster. You should upload required jar files to HDFS before running the job. This is the main difference between the Livy API and spark-submit.


- Spark Packages
```
bin/pyspark --master spark://spark-master:7077 --packages ml.combust.mleap:mleap-spark_2.11:0.12.0
```
