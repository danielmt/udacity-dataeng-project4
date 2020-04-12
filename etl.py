import configparser
from datetime import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import (
    year,
    month,
    dayofmonth,
    hour,
    weekofyear,
    dayofweek,
)
from pyspark.sql.types import TimestampType

from schema import SONG_DATA_SCHEMA, LOG_DATA_SCHEMA


config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["CLUSTER"].get("AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config["CLUSTER"].get("AWS_SECRET_ACCESS_KEY")


def create_local_spark_session() -> SparkSession:
    return SparkSession \
        .builder \
        .master("local") \
        .appName("Sparkify ETL") \
        .getOrCreate()


def create_spark_session() -> SparkSession:
    return SparkSession \
        .builder \
        .appName("Sparkify ETL") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()


def process_song_data(spark: SparkSession, input_data: str, output_data: str) -> None:
    # get filepath to song data file
    song_data = os.path.join(input_data, "song-data", "*", "*", "*", "*.json")

    # read song data file
    df = spark.read.json(song_data, schema=SONG_DATA_SCHEMA)

    # extract columns to create songs table
    songs_table = df.select(
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration"
    )

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        os.path.join(output_data, "songs_table.parquet"),
        mode="overwrite",
        partitionBy=["year", "artist_id"]
    )

    # extract columns to create artists table
    artists_table = df.select(
        "artist_id",
        col("artist_name").alias("name"),
        col("artist_location").alias("location"),
        col("artist_latitude").alias("latitude"),
        col("artist_longitude").alias("longitude")
    ).drop_duplicates()

    # write artists table to parquet files
    artists_table.write.parquet(
        os.path.join(output_data, "artists_table.parquet"),
        mode="overwrite"
    )


def process_log_data(spark: SparkSession, input_data: str, output_data: str) -> None:
    # get filepath to log data file
    log_data = os.path.join(input_data, "log-data", "*", "*", "*.json")

    # read log data file
    df = spark.read.json(log_data, schema=LOG_DATA_SCHEMA)

    # filter by actions for song plays
    df = df.filter(col("page") == "NextSong")

    # extract columns for users table
    users_table = df.select(
        col("userId").alias("user_id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        "gender",
        "level"
    ).drop_duplicates()

    # write users table to parquet files
    users_table.write.parquet(
        os.path.join(output_data, "users_table.parquet"),
        mode="overwrite"
    )

    # create datetime column from original timestamp column
    get_datetime = udf(
        lambda x: datetime.utcfromtimestamp(x / 1000),
        TimestampType()
    )
    df = df.withColumn("start_time", get_datetime("ts"))

    # extract columns to create time table
    time_table = (
        df
        .withColumn("hour", hour("start_time"))
        .withColumn("day", dayofmonth("start_time"))
        .withColumn("week", weekofyear("start_time"))
        .withColumn("month", month("start_time"))
        .withColumn("year", year("start_time"))
        .withColumn("weekday", dayofweek("start_time"))
        .select(
            "start_time",
            "hour",
            "day",
            "week",
            "month",
            "year",
            "weekday"
        )
        .drop_duplicates(["year", "month", "day", "hour"])
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(
        os.path.join(output_data, "time_table.parquet"),
        mode="overwrite",
        partitionBy=["year", "month"]
    )

    # read in song data to use for songplays table
    artists_df = spark.read.parquet(
        os.path.join(output_data, "artists_table.parquet")
    )
    songs_df = spark.read.parquet(
        os.path.join(output_data, "songs_table.parquet")
    )

    songs = (
        songs_df
        .join(artists_df, "artist_id", "inner")
        .select(
            "song_id",
            "title",
            "artist_id",
            col("name").alias("artist_name"),
            "duration"
        )
    )

    songplays_table = (
        df
        .join(songs, [
            df.song == songs.title,
            df.artist == songs.artist_name,
            df.length == songs.duration
        ], "left")
    )
    songplays_table = (
        songplays_table
        .select(
            monotonically_increasing_id().alias("songplay_id"),
            "start_time",
            col("userId").alias("user_id"),
            "level",
            "song_id",
            "artist_id",
            col("sessionId").alias("session_id"),
            "location",
            col("userAgent").alias("user_agent"),
            month("start_time").alias("month"),
            year("start_time").alias("year")
        )
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(
        os.path.join(output_data, "songplays_table.parquet"),
        mode="overwrite",
        partitionBy=["year", "month"]
    )


def main() -> None:
    spark = create_spark_session()
    # spark = create_local_spark_session()

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dmt-dend-sparkify/output/"

    # input_data = "/home/glitch/udacity/project-4-data-lake/"
    # output_data = "/home/glitch/udacity/project-4-data-lake/output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
