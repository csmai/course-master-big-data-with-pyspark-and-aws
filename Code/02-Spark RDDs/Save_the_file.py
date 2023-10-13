# Databricks notebook source
from pyspark import SparkConf, SparkContext

# Specify the full path of Sample.txt
file_path = (
    "file:///home/csmai/course-master-big-data-with-pyspark-and-aws/Code/Sample.txt"
)


# Configure
conf = SparkConf().setAppName("Read and write sample file")
sc = SparkContext.getOrCreate(conf=conf)
rdd = sc.textFile(file_path)
rdd = rdd.repartition(5)
rdd2 = rdd.flatMap(lambda line: line.split())

# Commands
print(rdd2.collect())
print(rdd2.getNumPartitions())

rdd2 = rdd2.coalesce(3)
# Specify the output directory
output_dir = "file:///home/csmai/course-master-big-data-with-pyspark-and-aws/Code/output/partition_test3"
# Save
rdd2.saveAsTextFile(output_dir)

rdd_read = sc.textFile(output_dir)
# Commands
print(rdd_read.collect())
print(rdd_read.getNumPartitions())
