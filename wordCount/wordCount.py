import sys
import os
from pyspark import SparkContext, SparkConf


if __name__ == "__main__":
    input_bucket = sys.argv[1]
    output_bucket = sys.argv[2]
    # create Spark context with necessary configuration
    sc = SparkContext("local", "PySpark Word Count Example")

    # read data from text file and split each line into words
    words = sc.textFile(input_bucket).flatMap(lambda line: line.split(" "))
    # count the occurrence of each word
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    # save the counts to output
    if (os.path.exists(output_bucket)):
        os.remove(output_bucket)
    wordCounts.saveAsTextFile(output_bucket)
    print(wordCounts.collect())
