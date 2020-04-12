from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    LongType,
)

SONG_DATA_SCHEMA = StructType([
    StructField("song_id", StringType()),
    StructField("title", StringType()),
    StructField("duration", DoubleType()),
    StructField("year", IntegerType()),
    StructField("num_songs", IntegerType()),
    StructField("artist_id", StringType()),
    StructField("artist_name", StringType()),
    StructField("artist_location", StringType()),
    StructField("artist_latitude", DoubleType()),
    StructField("artist_longitude", DoubleType()),
])

LOG_DATA_SCHEMA = StructType([
    StructField("auth", StringType()),
    StructField("userId", StringType()),
    StructField("registration", DoubleType()),
    StructField("level", StringType()),
    StructField("firstName", StringType()),
    StructField("lastName", StringType()),
    StructField("gender", StringType()),
    StructField("location", StringType()),
    StructField("sessionId", IntegerType()),
    StructField("ts", LongType()),
    StructField("page", StringType()),
    StructField("method", StringType()),
    StructField("status", IntegerType()),
    StructField("userAgent", StringType()),
    StructField("itemInSession", IntegerType()),
    StructField("artist", StringType()),
    StructField("song", StringType()),
    StructField("length", DoubleType()),
])
