import logging
import os
import re
import shutil
import uuid
from datetime import datetime, timedelta

import boto3
import emoji
import pandas as pd
from googleapiclient.discovery import build
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, current_timestamp, lit, to_date, udf)
from pyspark.sql.types import (DateType, IntegerType, LongType, StringType,
                               StructField, StructType)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.operators.bash import BashOperator

# Define the DAG and its default arguments
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # Whether to depend on past DAG runs
    'email_on_failure': False,  # Disable email notifications on failure
    'email_on_retry': False,  # Disable email notifications on retry
    'retries': 1,  # Number of retries
    'retry_delay': timedelta(minutes=5),  # Delay between retries
    'start_date': datetime(2023, 6, 10, 0, 0, 0),  # Runs everyday at midnight (00:00) UTC
}

dag = DAG(
    'youtube_etl_dag',  # DAG identifier
    default_args=default_args,  # Assign default arguments
    description='A simple ETL DAG',  # Description of the DAG
    schedule=timedelta(days=1),  # Schedule interval: daily
    catchup=False,  # Do not catch up on missed DAG runs
)


# Python callable function to extract data from YouTube API
def extract_data(**kwargs):
    api_key = kwargs['api_key']
    region_codes = kwargs['region_codes']
    category_ids = kwargs['category_ids']

    df_trending_videos = fetch_data(api_key, region_codes, category_ids)
    current_date = datetime.now().strftime("%Y%m%d")
    output_path = f'/opt/airflow/Youtube_Trending_Data_Raw_{current_date}'
    # Save DataFrame to CSV file
    df_trending_videos.to_csv(output_path, index=False)


def fetch_data(api_key, region_codes, category_ids):
    """
    Fetches trending video data for multiple countries and categories from YouTube API.
    Returns a pandas data frame containing video data.
    """
    # Initialize an empty list to hold video data
    video_data = []

    # Build YouTube API service
    youtube = build('youtube', 'v3', developerKey=api_key)

    for region_code in region_codes:
        for category_id in category_ids:
            # Initialize the next_page_token to None for each region and category
            next_page_token = None
            while True:
                # Make a request to the YouTube API to fetch trending videos
                request = youtube.videos().list(
                    part='snippet,contentDetails,statistics',
                    chart='mostPopular',
                    regionCode=region_code,
                    videoCategoryId=category_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                response = request.execute()
                videos = response['items']

                # Process each video and collect data
                for video in videos:
                    video_info = {
                        'region_code': region_code,
                        'category_id': category_id,
                        'video_id': video['id'],
                        'title': video['snippet']['title'],
                        'published_at': video['snippet']['publishedAt'],
                        'view_count': video['statistics'].get('viewCount', 0),
                        'like_count': video['statistics'].get('likeCount', 0),
                        'comment_count': video['statistics'].get('commentCount', 0),
                        'channel_title': video['snippet']['channelTitle']
                    }
                    video_data.append(video_info)

                # Get the next page token, if there are more pages of results
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break

    return pd.DataFrame(video_data)


def preprocess_data_pyspark_job():
    spark = SparkSession.builder.appName('YouTubeTransform').getOrCreate()
    current_date = datetime.now().strftime("%Y%m%d")
    output_path = f'/opt/airflow/Youtube_Trending_Data_Raw_{current_date}'
    df = spark.read.csv(output_path, header=True)

    # Define UDF to remove hashtag data, emojis
    def clean_text(text):
        if text is not None:
            # Remove emojis
            text = emoji.demojize(text, delimiters=('', ''))

            # Remove hashtags and everything after them
            if text.startswith('#'):
                text = text.replace('#', '').strip()
            else:
                split_text = text.split('#')
                text = split_text[0].strip()

            # Remove extra double quotes and backslashes
            text = text.replace('\\"', '')  # Remove escaped quotes
            text = re.sub(r'\"+', '', text)  # Remove remaining double quotes
            text = text.replace('\\', '')  # Remove backslashes

            return text.strip()  # Strip any leading or trailing whitespace

        return text

    # Register UDF
    clean_text_udf = udf(clean_text, StringType())

    # Clean the data
    df_cleaned = df.withColumn('title', clean_text_udf(col('title'))) \
        .withColumn('channel_title', clean_text_udf(col('channel_title'))) \
        .withColumn('published_at', to_date(col('published_at'))) \
        .withColumn('view_count', col('view_count').cast(LongType())) \
        .withColumn('like_count', col('like_count').cast(LongType())) \
        .withColumn('comment_count', col('comment_count').cast(LongType())) \
        .dropna(subset=['video_id'])

    # Generate the filename based on the current date
    current_date = datetime.now().strftime("%Y%m%d")
    output_path = f'/opt/airflow/Transformed_Youtube_Data_{current_date}'

    # Write cleaned DataFrame to the specified path
    df_cleaned.write.csv(output_path, header=True, mode='overwrite')


def load_data_to_s3(**kwargs):
    bucket_name = kwargs['bucket_name']
    today = datetime.now().strftime('%Y/%m/%d')
    prefix = f"processed-data/{today}"
    current_date = datetime.now().strftime("%Y%m%d")
    local_dir_path = f'/opt/airflow/Transformed_Youtube_Data_{current_date}'
    upload_to_s3(bucket_name, prefix, local_dir_path)


def upload_to_s3(bucket_name, prefix, local_dir_path):
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    for root, dirs, files in os.walk(local_dir_path):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                s3_key = f"{prefix}/{file}"
                logging.info(f"Uploading {file_path} to s3://{bucket_name}/{s3_key}")
                s3_client.upload_file(file_path, bucket_name, s3_key)


def consolidate_data():
    """
    Consolida los datos transformados y agrega columnas de control
    """
    spark = SparkSession.builder.appName('YouTubeConsolidate').getOrCreate()
    current_date = datetime.now().strftime("%Y%m%d")
    input_path = f'/opt/airflow/Transformed_Youtube_Data_{current_date}'
    
    # Leer los datos transformados
    df = spark.read.csv(input_path, header=True)
    
    # Generar un UUID único para el batch_id
    batch_id = str(uuid.uuid4())
    
    # Agregar columnas de control
    df_with_control = df.withColumn('ingestion_date', current_timestamp()) \
        .withColumn('batch_id', lit(batch_id))
      # Guardar el DataFrame consolidado
    output_path = '/data/youtube_data.csv'
    
    # Verificar si el archivo existe para determinar si necesitamos incluir el encabezado
    header = not os.path.exists(output_path)
    
    # Convertir a pandas y guardar en modo append
    df_with_control.toPandas().to_csv(output_path, 
                                     mode='a',  # append mode
                                     header=header,  # solo incluir header si es un archivo nuevo
                                     index=False)


# Define extract task for the DAG
extract_task = PythonOperator(
    task_id='extract_data_from_youtube_api',
    python_callable=extract_data,
    op_kwargs={
        'api_key': os.getenv('YOUTUBE_API_KEY'),
        'region_codes': ['CO', 'US', 'GB', 'IN', 'AU', 'NZ'],
        'category_ids': ['1', '2', '10', '15', '20', '22', '23', '28']
    },
    dag=dag,
)

# Define preprocessing task for the DAG
preprocess_data_pyspark_task = PythonOperator(
    task_id='preprocess_data_pyspark_task',
    python_callable=preprocess_data_pyspark_job,
    dag=dag
)

# # Define Load Task for DAG
# load_data_to_s3_task = PythonOperator(
#     task_id='load_data_to_s3',
#     python_callable=load_data_to_s3,
#     op_kwargs={
#         'bucket_name': 'your-s3bucket-name'
#     },
#     dag=dag
# )

# Define consolidation task for the DAG
consolidate_data_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)

# Define build DBT image task
build_dbt_image_task = DockerOperator(
    task_id='build_dbt_image',
    api_version='auto',
    auto_remove='success',
    command='build -t etl_youtube_dbt:latest -f Dockerfile.dbt .',
    image='docker:latest',
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge',
    privileged=True,
    mount_tmp_dir=False,
    mounts=[
        Mount(source='/var/run/docker.sock', target='/var/run/docker.sock', type='bind'),
        Mount(source='/home/satoru/repos/lab/etl-youtube', target='/workspace', type='bind'),
    ],
    working_dir='/workspace',
    dag=dag,
)

# Define DBT task
dbt_task = DockerOperator(
    task_id='run_dbt',
    image='etl_youtube_dbt:latest',
    api_version='auto',
    auto_remove='success',
    force_pull=False,
    mount_tmp_dir=False,
    command=["run",
             "--profiles-dir", "/root/.dbt",
             "--project-dir",  "/dbt"],
    network_mode='bridge',
    mounts=[
        Mount(source='/home/satoru/repos/lab/etl-youtube/dbt', target='/dbt', type='bind'),
        Mount(source='/home/satoru/repos/lab/etl-youtube/dbt/profiles.yml', target='/root/.dbt/profiles.yml', type='bind'),
        Mount(source='/home/satoru/repos/lab/etl-youtube/data', target='/data', type='bind'),
    ],
    working_dir='/dbt',
    environment={
        'DBT_PROFILES_DIR': '/root/.dbt',
        'DBT_PROJECT_DIR': '/dbt'
    },
    dag=dag,
)

# Update task dependencies to include build and DBT
extract_task >> preprocess_data_pyspark_task >> consolidate_data_task >> build_dbt_image_task >> dbt_task


