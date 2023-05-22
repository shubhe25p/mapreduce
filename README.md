# Hadoop and Spark Exploration

This repository is aimed at exploring Hadoop and Spark through four programs: palindrome in Hadoop and Spark, word co-occurring in Hadoop and Spark. Below are the steps to install and use the programs on your Hadoop cluster.

## Prerequisites
- Maven should be installed on your machine.
- Hadoop and Spark should be set up and running on your cluster.

## Installation

1. Install Maven from source.

```
# Replace <maven-source> with the source path of Maven.
$ cd <maven-source>
$ ./configure
$ make
$ sudo make install

```

1. Install Maven from source
2. mvn -v
3. mvn clean package

After a JAR file is created from the commands above follow this steps on the Hadoop cluster:

Store data in HDFS with 
```
hdfs dfs -put /path/to/data/local /path/to/data/hdfs
hdfs dfs -rm -r /path/to/data/hdfs/to/remove
```
To run Hadoop jobs, run:
```
/bin/hadoop jar path/to/jar/file org.apache.hadoop.examples.ClassName path/to/data/hdfs output/path/hdfs
```
Careful: Classname should be same as filename in Java

for spark jobs:

```
spark-submit --class org.apache.hadoop.examples.ClassName --master yarn --deploy-mode cluster path/to/jar/file path/to/data/hdfs output/path/hdfs
```