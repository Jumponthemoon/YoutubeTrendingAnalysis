#!/bin/bash

if [ $# -ne 4 ]; then
    echo "Invalid number of parameters!"
    echo "Usage: ./driver.sh [input_location] [output_location] [firstCountry] [secondCountry]"
    exit 1
fi

# $@ holds list of all arguments passed to the script, cut -d "" cut according to the space
#country_name=`cut -d" " -f4- <<< "$@"`
#compare_country_name=`cut -d" " -f5- <<< "$@"`
# -D mapreduce.job.reduces=3 \

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.9.0.jar \
-D mapreduce.job.name='Category and Trending Correlation' \
-file mapper.py \
-mapper 'python3 mapper.py '$3' '$4 \
-file reducer.py \
-reducer 'python3 reducer.py '$3' '$4 \
-input $1 \
-output $2
