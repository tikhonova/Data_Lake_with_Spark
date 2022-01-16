import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.window import Window

config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'KEY')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'SECRET')
print("-----Connected-----")


def create_spark_session():
    """
    Creates a spark object
    :rtype: object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes and partitions song data
    :rtype: object
    """
    song_data = input_data + 'song_data/A/B/C/TRABCEI128F424C983.json'

    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data_table")  #
    songs_table = spark.sql("""
                            SELECT sdtn.song_id, 
                            sdtn.title,
                            sdtn.artist_id,
                            sdtn.year,
                            sdtn.duration
                            FROM song_data_table sdtn
                            WHERE song_id IS NOT NULL
                        """)

    songs_table.write.mode('overwrite'). \
        partitionBy("year", "artist_id"). \
        parquet(output_data + 'songs_table/')
    artists_table = spark.sql("""
                                SELECT DISTINCT arti.artist_id, 
                                arti.artist_name,
                                arti.artist_location,
                                arti.artist_latitude,
                                arti.artist_longitude
                                FROM song_data_table arti
                                WHERE arti.artist_id IS NOT NULL
                            """)

    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
    Processes and splits log data into standalone tables
    :rtype: object
    """
    log_data = input_data + 'log_data/*/*/*.json'
    df = spark.read.json(log_data)

    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView("log_data_table")

    users_table = spark.sql("""
                                SELECT 
                                    DISTINCT(l.userId) AS user_id, 
                                    l.firstName AS first_name,
                                    l.lastName AS last_name,
                                    l.gender AS gender,
                                    l.level AS level
                                FROM log_data_table l
                                WHERE l.userId IS NOT NULL
                            """)

    users_table.write.mode('overwrite').parquet(output_data + 'users_table/')

    artists_table = spark.sql("""
                            SELECT DISTINCT l.userId as user_id, 
                            l.firstName as first_name,
                            l.lastName as last_name,
                            l.gender as gender,
                            l.level as level
                            FROM log_data_table l
                            WHERE l.userId IS NOT NULL
                        """)

    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table/')

    time_table = spark.sql("""
                            SELECT 
                            A.start_time_sub as start_time,
                            hour(A.start_time_sub) as hour,
                            dayofmonth(A.start_time_sub) as day,
                            weekofyear(A.start_time_sub) as week,
                            month(A.start_time_sub) as month,
                            year(A.start_time_sub) as year,
                            dayofweek(A.start_time_sub) as weekday
                            FROM
                            (SELECT to_timestamp(l.ts/1000) as start_time_sub
                            FROM log_data_table l
                            WHERE l.ts IS NOT NULL
                            ) A
                        """)

    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'time_table/')
    song_df = spark.read.parquet(output_data + 'songs_table/')
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                to_timestamp(l.ts/1000) as start_time,
                                month(to_timestamp(l.ts/1000)) as month,
                                year(to_timestamp(l.ts/1000)) as year,
                                l.userId as user_id,
                                l.level as level,
                                s.song_id as song_id,
                                s.artist_id as artist_id,
                                l.sessionId as session_id,
                                l.location as location,
                                l.userAgent as user_agent
                                FROM log_data_table l
                                JOIN song_data_table s on l.artist = s.artist_name and l.song = s.title
                            """)
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'songplays_table/')


def main():
    """
    Runs song and log functions
    :rtype: object
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "./Results/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
    spark.stop()
    print('Success')


if __name__ == "__main__":
    main()