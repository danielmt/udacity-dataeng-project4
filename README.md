# Project: Data Lake

A music streaming startup, Sparkify, has grown their user base and song database
even more and want to move their data warehouse to a data lake. Their data
resides in S3, in a directory of JSON logs on user activity on the app, as well
as a directory with JSON metadata on the songs in their app.

This project will extract data from S3, transform it using Spark and store data
back into a S3 data lake using the parquet format.

## Input data format

### Song data format

``` json
{
  "artist_id": "ARAJPHH1187FB5566A",
  "artist_latitude": 40.7038,
  "artist_location": "Queens, NY",
  "artist_longitude": -73.83168,
  "artist_name": "The Shangri-Las",
  "duration": 164.80608,
  "num_songs": 1,
  "song_id": "SOYTPEP12AB0180E7B",
  "title": "Twist and Shout",
  "year": 1964
}
```

### Event data format

``` json
{
  "artist": "Explosions In The Sky",
  "auth": "Logged In",
  "firstName": "Layla",
  "gender": "F",
  "itemInSession": 87,
  "lastName": "Griffin",
  "length": 220.3424,
  "level": "paid",
  "location": "Lake Havasu City-Kingman, AZ",
  "method": "PUT",
  "page": "NextSong",
  "registration": 1541057188796,
  "sessionId": 984,
  "song": "So Long_ Lonesome",
  "status": 200,
  "ts": 1543449470796,
  "userAgent": "\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36\"",
  "userId": "24"
}
```

## Schema

### Fact Tables

#### songplays

Records in event data associated with song plays i.e. records with `page`
`NextSong`.

| Column      | Type      |
| ----------- | --------- |
| songplay_id | Integer   |
| start_time  | Timestamp |
| user_id     | String    |
| level       | String    |
| song_id     | String    |
| artist_id   | String    |
| session_id  | Integer   |
| location    | String    |
| user_agent  | String    |
| year        | Integer   |
| month       | Integer   |

partitioned by year and month.

### Dimension Tables

#### users

Users in the app.

| Column     | Type    |
| ---------- | ------- |
| user_id    | String  |
| first_name | String  |
| last_name  | String  |
| gender     | String  |
| level      | String  |

#### songs

Songs in music database.

| Column    | Type    |
| --------- | ------- |
| song_id   | String  |
| title     | String  |
| artist_id | String  |
| year      | Integer |
| duration  | Double  |

partitioned by: year and artist_id

#### artists

Artists in music database.

| Column    | Type   |
| --------- | ------ |
| artist_id | String |
| name      | String |
| location  | String |
| latitude  | Double |
| longitude | Double |

#### time

Timestamps of records in songplays broken down into specific units.

| Column     | Type      |
| ---------- | --------- |
| start_time | Timestamp |
| hour       | Integer   |
| day        | Integer   |
| week       | Integer   |
| month      | Integer   |
| year       | Integer   |
| weekday    | Integer   |

partitioned by: year and month

## Build

Pre-requisite:

- AWS EMR Cluster 6.0 (Hadoop 3.2, Spark 2.4, Python 3)

## Configuration

Copy `dl.cfg.example` to `dl.cfg`, and fill the CLUSTER settings for S3 access.

## Running

To execute the pipeline, run from EMR:

``` sh
/usr/bin/spark-submit --master yarn etl.py
```
