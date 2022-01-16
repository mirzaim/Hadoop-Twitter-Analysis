# Hadoop Twitter Analysis

This project uses Hadoop and MapReduce to analyze the "US Election 2020 Tweets" dataset. It demonstrates the power of distributed computing for processing and analyzing big data related to the 2020 US presidential election.

## Dataset

The analysis is based on the "US Election 2020 Tweets" dataset, which can be found on Kaggle:
[US Election 2020 Tweets](https://www.kaggle.com/datasets/manchunhui/us-election-2020-tweets)

## Project Overview

The project consists of three main MapReduce jobs:

1. **Tweet Like and Retweet Counter**: Calculates the total number of likes and retweets for tweets mentioning Joe Biden and Donald Trump.

2. **Tweet Country Location Analyzer**: Analyzes the percentage of tweets from various countries mentioning each candidate.

3. **Tweet Geo-Location Analyzer**: Uses geographical coordinates to determine tweet origins, focusing on tweets from the USA and France.

## Setup

### HDFS Setup

1. Create the necessary HDFS directories:

```bash
hdfs dfs -mkdir -p /user/hadoop/input
```

2. Upload the input files to HDFS:

```bash
hdfs dfs -put input/*.csv /user/hadoop/input
```

## Running the MapReduce Jobs

### 1. Tweet Like and Retweet Counter

```bash
hadoop jar hadoop-twitter-analysis.jar TweetCounterDriver /user/hadoop/input /user/hadoop/output/like_retweet_counts
```

### 2. Tweet Country Location Analyzer

```bash
hadoop jar hadoop-twitter-analysis.jar TweetCountryLocationDriver /user/hadoop/input /user/hadoop/output/country_analysis
```

### 3. Tweet Geo-Location Analyzer

```bash
hadoop jar hadoop-twitter-analysis.jar TweetGeoLocationDriver /user/hadoop/input /user/hadoop/output/geo_location_analysis
```

## Output

The output of each job will be stored in the respective output directory in HDFS. You can view the results using:

```bash
hdfs dfs -cat /user/hadoop/output/<job_output_directory>/*
```
