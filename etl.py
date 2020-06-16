import os
import configparser
import pyspark.sql.functions as f
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import types as t
from pyspark.sql.functions import udf


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

song_data_path= config['S3']['SONG_DATA_UDACITY']
log_data_path = config['S3']['LOG_DATA_UDACITY']
output_path = config['S3']['SONG_OUTPUT_PATH']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def get_time():
    """
    Returns current time formatted as yyyy-mm-dd_hh-mm-ss-ms

    Output:
    string - String of current time
    """
    n = datetime.now()

    timestamp = f"""
    {n.year}-{n.month}-{n.day}_{n.hour}-{n.minute}-{n.second}-{n.microsecond}
    """.strip()
    
    return timestamp

@udf(t.TimestampType())
def make_timestamp(ts):
    """
    Spark UDF to convert timestamp to datetime
    """
    return datetime.fromtimestamp(ts / 1000.0)

@udf(t.StringType())
def make_datetime(ts):
    """
    Spark UDF to convert timestamp to datetime and string format as yyyy-mm-dd hh:mm:ss
    """
    return datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

def process_song_data(spark, input_data, output_data):
    """
    Read data from the S3 bucket for song data, use Spark to create tables and save to
    output S3 bucket in parquet

    Input:
    spark: Spark session
    input_data: Path to input data
    output_data: Path to output data location

    Output:
    None
    """
    song_data = spark.read.json(input_data)
    print('Song data schema:')
    song_data.printSchema()
    song_data.createOrReplaceTempView('song_data')

    # songs table
    songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM song_data
        ORDER BY song_id
    """)
    print('songs table schema:')
    songs_table.printSchema()
    songs_table_path = f'{output_data}songs_table.{get_time()}'
    print(f'Writing songs table as parquet to {songs_table_path}..')
    songs_table.write.partitionBy('year', 'artist_id').parquet(songs_table_path)

    # artists table
    artists_table = spark.sql("""
        SELECT
            artist_id AS artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
        FROM song_data
        ORDER BY artist_id DESC
    """)
    print('artists table schema:')
    artists_table.printSchema()
    artists_table_path = f'{output_data}artists_table.{get_time()}'
    print(f'Writing artists table as parquet to {artists_table_path}..')
    artists_table.write.parquet(artists_table_path)



def process_log_data(spark, input_data_songs, input_data_logs, output_data):
    """
    Read data from the S3 bucket for song and log data, use Spark to create tables and save to
    output S3 bucket in parquet

    Input:
    spark: Spark session
    input_data_songs: Path to songs input data
    input_data_logs: Path to logs input data
    output_data: Path to output data location

    Output:
    None
    """
    song_data = spark.read.json(input_data_songs)
    # print('Song data schema:')
    # song_data.printSchema()
    song_data.createOrReplaceTempView('song_data')
    log_data = spark.read.json(input_data_logs)
    log_data = log_data.filter(log_data.page == 'NextSong')
    print('Log data schema:')
    log_data.printSchema()
    log_data.createOrReplaceTempView('log_data')

    # users table
    users_table = spark.sql("""
        SELECT 
            DISTINCT userId AS user_id,
            firstName AS first_name,
            lastName AS last_name,
            gender,
            level
        FROM log_data
        ORDER BY last_name
    """)
    print('users table schema:')
    users_table.printSchema()
    users_table_path = f'{output_data}users_table.{get_time()}'
    print(f'Writing users table as parquet to {users_table_path}..')
    users_table.write.parquet(users_table_path)

    # time table
    log_data_time = log_data.withColumn('time', make_timestamp('ts'))
    log_data_time_final = log_data_time.withColumn('datetime', make_datetime('ts'))
    log_data_time_final.createOrReplaceTempView('log_data_time')
    time_table = spark.sql("""
        SELECT DISTINCT datetime AS start_time,
            hour(time) AS hour,
            day(time) AS day,
            weekofyear(time) AS week,
            month(time) AS month,
            year(time) AS year,
            dayofweek(time) AS weekday
        FROM log_data_time
        ORDER BY start_time
    """)
    print('time table schema:')
    time_table.printSchema()
    time_table_path = f'{output_data}time_table.{get_time()}'
    print(f'Writing time table as parquet to {time_table_path}..')
    time_table.write.partitionBy("year", "month").parquet(time_table_path)

    # songplays table
    joined_data = log_data_time_final.join(song_data,
    (log_data_time_final.artist == song_data.artist_name) &
    (log_data_time_final.song == song_data.title)
    )
    joined_data = joined_data.withColumn('songplay_id', f.monotonically_increasing_id())
    joined_data.createOrReplaceTempView('joined_data')
    songplays_table = spark.sql("""
        SELECT songplay_id AS songplay_id,
            time AS start_time,
            userId as user_id,
            level AS level,
            song_id AS song_id,
            artist_id AS artist_id,
            sessionId AS session_id,
            location AS location,
            userAgent AS user_agent
        FROM joined_data
        ORDER BY (user_id, session_id)
    """)
    print('songplays table schema:')
    songplays_table.printSchema()
    songplays_table_path = f'{output_data}songplays_table.{get_time()}'
    print(f'Writing songplays table as parquet to {songplays_table_path}..')
    songplays_table.write.partitionBy("year", "month").parquet(songplays_table_path)




def main():
    spark = create_spark_session()
    print('Spark session created..')
    print(spark)
    print('Processing for artists and songs table..')
    process_song_data(spark, song_data_path, output_path)
    print('Porcessing for users, time and songplays table..')    
    process_log_data(spark, song_data_path, log_data_path, output_path)


if __name__ == "__main__":
    main()
