# Databricks notebook source
from pyspark import SparkConf, SparkContext

# Specify the local file path correctly
file_path = "file:///C:/Users/User/Desktop/OneDrive/pylearn/course-master-big-data-with-pyspark-and-aws/Code/Sample.txt"

# Configure
conf = SparkConf().setAppName("Read and write sample file")
sc = SparkContext.getOrCreate(conf=conf)
rdd = sc.textFile(file_path)

# Commands
print(rdd.collect())

# Specify the output directory
output_dir = "file:///C:/Users/User/Desktop/OneDrive/pylearn/course-master-big-data-with-pyspark-and-aws/Code/output/saved_sample"
# Save
rdd.flatMap(lambda line: line.split()).saveAsTextFile(output_dir)
