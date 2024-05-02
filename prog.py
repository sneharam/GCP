from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage

def create_bucket(bucket_name):
    """Creates a new bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    try:
        bucket.create()
        print(f'Bucket {bucket_name} created successfully')
    except Exception as e:
        print(f'Error creating bucket: {e}')

def upload_object(bucket_name, file_path, destination_blob_name):
    """Uploads an object to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    try:
        blob.upload_from_filename(file_path)
        print(f'File {file_path} uploaded to {destination_blob_name} in bucket {bucket_name}')
    except Exception as e:
        print(f'Error uploading file: {e}')

def delete_file(bucket_name, blob_name):
    """Deletes a file from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    try:
        blob.delete()
        print(f'File {blob_name} deleted from bucket {bucket_name}')
    except Exception as e:
        print(f'Error deleting file: {e}')


# Define DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 20),
    'retries': 1
}

dag = DAG(
    'upload_to_gcs',
    default_args=default_args,
    description='DAG to upload objects to Google Cloud Storage',
    schedule_interval=None,
)

# Tasks
create_bucket_task = PythonOperator(
    task_id='create_bucket',
    python_callable=create_bucket,
    op_kwargs={'bucket_name': 'your-bucket-name'},
    dag=dag,
)

upload_object_task = PythonOperator(
    task_id='upload_object',
    python_callable=upload_object,
    op_kwargs={
        'bucket_name': 'your-bucket-name',
        'file_path': r'C:\Users\2320393\OneDrive - Cognizant\Desktop\sneha.txt',
        'destination_blob_name': 'uploaded_file.txt'
    },
    dag=dag,
)

# Define task dependencies
create_bucket_task >> upload_object_task
