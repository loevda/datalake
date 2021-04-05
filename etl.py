import configparser
from datetime import datetime
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, \
    weekofyear, date_format, dayofweek, monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

#fixing config errors
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


# setting up the session
def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("Data Lake with Spark") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
        .getOrCreate()
    return spark




def process_song_data(spark, input_data, output_data):
    """
    Extract data from JSON files and
    write partitioned parquet files to S3
    :param spark: The Spark session object
    :param input_data: Source JSON files on S3
    :param output_data: Target S3 bucket where to write Parquet files
    :return: None
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    # songs - songs in music database:
    # song_id, title, artist_id, year, duration
    songs_table = \
        df.select('song_id', 'title', 'artist_id', 'year', 'duration').where(
            col("song_id").isNotNull()).distinct()


    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(
        os.path.join(output_data, 'songs.parquet'), 'overwrite')

    # extract columns to create artists table
    # artists - artists in music database
    # artist_id, name, location, lattitude, longitude
    # renaming columns where needed
    artists_table = \
        df.select(
            'artist_id', 'artist_name',
            'artist_location', 'artist_latitude',
            'artist_longitude').where(col("artist_id").isNotNull()).withColumnRenamed(
            "artist_name", "name"
        ).withColumnRenamed(
            "artist_location", "location"
        ).withColumnRenamed(
            "artist_latitude", "latitude"
        ).withColumnRenamed(
            "artist_longitude", "longitude"
        ).distinct()

    # write artists table to parquet files
    artists_table.write.parquet(
        os.path.join(output_data, 'artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Extract log data from JSON files and write
    data to parquet files on S3
    :param spark: The Spark session object
    :param input_data: Source JSON files on S3
    :param output_data: Target S3 bucket where to write Parquet files
    :return: None
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')
    # # song data needed for join
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    # users - users in the app
    # user_id, first_name, last_name, gender, level
    users_table = df.select(
        'userId', 'firstName',
        'lastName', 'gender', 'level').where(col("userId").isNotNull()).withColumnRenamed(
        "userId", "user_id"
    ).withColumnRenamed(
        "firstName", "first_name"
    ).withColumnRenamed(
        "lastName", "last_name"
    ).distinct()

    # write users table to parquet files
    users_table.write.parquet(
        os.path.join(output_data, 'users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x)/1000)
    df = df.withColumn('start_time', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(x)/1000))
    df = df.withColumn('datetime', get_datetime('ts'))

    # extract columns to create time table
    # time - timestamps of records in songplays broken down into specific units
    # start_time, hour, day, week, month, year, weekday
    time_table = df.select('start_time', 'datetime').withColumn(
        'hour', hour('datetime')
    ).withColumn(
        'day', dayofmonth('datetime')
    ).withColumn(
        'week', weekofyear('datetime')
    ).withColumn(
        'month', month('datetime')
    ).withColumn(
        'year', year('datetime')
    ).withColumn(
        'weekday', dayofweek('datetime')
    ).distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(
        os.path.join(output_data, 'time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data).alias('song_df')

    # extract columns from joined song and log datasets to create songplays table
    # songplays - records in log data associated with song plays i.e. records with page NextSong
    # songplay_id, start_time, user_id, level, song_id, artist_id,
    # session_id, location, user_agent
    songplays_table = df.join(song_df, col("artist")
                          == col("song_df.artist_name"), 'inner').select(
        col('start_time'),
        col('userId').alias('user_id'),
        col('level'),
        col('song_df.song_id').alias('song_id'),
        col('song_df.artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location'),
        col('userAgent').alias('user_agent'),
        year('datetime').alias('year'),
        month('datetime').alias('month')
    ).withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(
        os.path.join(output_data, 'songplays.parquet'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = config['AWS']['AWS_S3_OUTPUT_BUCKET']

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
