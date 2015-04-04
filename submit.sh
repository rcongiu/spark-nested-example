spark-submit --class nesting \
	--master local[2] \
	 --jars /Users/rcongiu/work/scala-hive/spark-csv-assembly-0.1.1.jar \
	target/scala-2.10/nested_2.10-1.0.jar
