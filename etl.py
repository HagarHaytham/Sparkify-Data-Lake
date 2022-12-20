import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import DateType,TimestampType
from pyspark.sql.functions import monotonically_increasing_id

@udf(TimestampType())
def get_timestamp(ts):
    return datetime.fromtimestamp(ts/1000.0)

@udf()
def get_datetime(ts):
    return datetime.fromtimestamp(ts/1000.0).strftime('%Y-%m-%d %H:%M:%S')

songs_select = """ SELECT  DISTINCT song_id,
                                    title,
                                    artist_id,
                                    year,
                                    duration
                    FROM stagingsongs
                    WHERE song_id IS NOT NULL
                """

artists_select = """ SELECT  DISTINCT artist_id AS artist_id,
                                      artist_name As name,
                                      artist_location AS location,
                                      artist_latitude AS latitude,
                                      artist_longitude AS longitude
                     FROM stagingsongs
                     WHERE artist_id IS NOT NULL   
                 """


users_select = """ SELECT  DISTINCT userId AS user_id,
                                    firstName AS first_name,
                                    lastName AS last_name,
                                    gender AS gender,
                                    level AS level
                    FROM stagingevents
                    WHERE userId IS NOT NULL
                    AND page = 'NextSong'
                """

time_select = """SELECT  DISTINCT datetime as start_time,
                                  hour ( timestamp) AS hour,
                                  day ( timestamp) AS day,
                                  weekofyear ( timestamp) AS week,
                                  month ( timestamp) AS month,
                                  year ( timestamp) AS year,
                                  weekday ( timestamp) AS weekday
                  FROM stagingevents
                  WHERE page = 'NextSong'
               """
songplays_select = """SELECT  monotonically_increasing_id() AS songplay_id,
                              ste.timestamp AS start_time,
                              month ( timestamp) AS month,
                              year ( timestamp) AS year,
                              ste.userId AS user_id,
                              ste.level AS level,
                              sts.song_id AS song_id, 
                              sts.artist_id AS artist_id,
                              ste.sessionId AS session_id,
                              ste.location AS location,
                              ste.userAgent AS user_agent
                      FROM stagingevents ste
                      JOIN stagingsongs sts
                      ON sts.title = ste.song 
                      AND sts.artist_name = ste.artist  
                      AND sts.duration = ste.length
                      WHERE ste.page = 'NextSong'  
                   """

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Input is a spark session, path to S3 bucket that contains the input data and path to S3 bucket that we should write the tables to.
    Here we read the songs data from S3 then using spark sql we extract the needed data to the songs and artists table
    Then write these two tables to the output S3 bucket.
    """
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("stagingsongs")
    
    # extract columns to create songs table
    songs_table = spark.sql(songs_select)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(output_data + "songs.parquet")

    # extract columns to create artists table
    artists_table = spark.sql(artists_select)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists.parquet")


def process_log_data(spark, input_data, output_data):
    """
    Input is a spark session, path to S3 bucket that contains the input data and path to S3 bucket that we should write the tables to.
    Here we read the events log data from S3 then using spark sql we extract the needed data to the users and time table.
    Then we read the songs data from S3 then join it with the log data to extract the songplays table.
    Then write these three tables to the output S3 bucket.
    """
    # get filepath to log data file
    log_data = input_data +"log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    df.createOrReplaceTempView("stagingevents")
    
    # filter by actions for song plays
    # df = 

    # extract columns for users table    
    users_table = spark.sql(users_select)
    
    # write users table to parquet files
    users_table.write.parquet( output_data + "songs/users.parquet")
    
    # remove all null timestamps
    df = spark.sql(""" SELECT * From stagingevents WHERE ts IS NOT NULL """)
    
    # create timestamp column from original timestamp column
    df =  df.withColumn("timestamp",get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    df = df.withColumn("datetime",get_datetime("ts"))
    
    df.createOrReplaceTempView("stagingevents")
    
    # extract columns to create time table
    time_table = spark.sql(time_select)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(output_data + "time.parquet")

    # read in song data to use for songplays table
    song_data = input_data+"song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)
    song_df.createOrReplaceTempView("stagingsongs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(songplays_select)

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    """
    Create spark session then pass it along the input data path and output data path 
    to the two functions that process both the events log and song data 
    to read them from s3 then do all the transformations
    then output the 5 new tables and write them to s3 bucket
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://my-sparkify123/sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
