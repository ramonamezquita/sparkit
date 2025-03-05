#!/bin/bash
set -e

cd $SPARK_HOME/jars

# AWS Java SDK For Amazon S3
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.779/aws-java-sdk-s3-1.12.779.jar

# AWS Java SDK For Amazon DynamoDB
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-dynamodb/1.12.779/aws-java-sdk-dynamodb-1.12.779.jar

# Hadoop-AWS
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# AWS Core
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.12.782/aws-java-sdk-core-1.12.782.jar