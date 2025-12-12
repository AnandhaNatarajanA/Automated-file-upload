import boto3
import csv
import os
import pymysql # <-- **CHANGE 1: Import pymysql instead of psycopg2**

# The lambda will need the 'pymysql' library included in its deployment package
# or as a Lambda Layer.

s3_client = boto3.client('s3')
sns = boto3.client('sns')
sns_topic_arn = os.environ['SNS_TOPIC_ARN']

def lambda_handler(event, context):
    conn = None # Initialize conn outside try block for proper cleanup
    try:
        # Get bucket and file details from event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        # Read CSV file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        # Decode the file content
        content = response['Body'].read().decode('utf-8').splitlines()
        reader = csv.DictReader(content)

        # Connect to RDS MySQL database
        # CHANGE 2: Use pymysql.connect instead of psycopg2.connect
        # The connection parameters (host, database, user, password) remain the same (from Environment Variables)
        conn = pymysql.connect(
            host=os.environ['DB_HOST'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            db=os.environ['DB_NAME'],
            # Optional: autocommit=True if you prefer, but conn.commit() is used below
            connect_timeout=10 
        )
        cur = conn.cursor()

        # Insert each record into table
        for row in reader:
            # CHANGE 3: The SQL query structure and the %s placeholder for values work with pymysql
            cur.execute(
                "INSERT INTO customers (id, name, email) VALUES (%s, %s, %s)",
                (row['id'], row['name'], row['email'])
            )

        conn.commit()
        cur.close()
        # The conn.close() is moved to the finally block for safety, but remains here for logic flow.
        conn.close() 
        conn = None # Set to None to prevent double-closing in finally

        # Publish success message
        sns.publish(
            TopicArn=sns_topic_arn,
            Subject="Data Insertion Success",
            Message=f"File {key} processed successfully and data inserted into RDS."
        )

    except Exception as e:
        # Publish failure message
        sns.publish(
            TopicArn=sns_topic_arn,
            Subject="Data Insertion Failed",
            Message=f"Error processing file {key}: {str(e)}"
        )
        raise e
    
    finally:
        # **BEST PRACTICE ADDITION:** Ensure connection is closed even if an error occurs
        if conn:
            conn.close()