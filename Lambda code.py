import boto3
import csv
import os
import psycopg2
s3_client = boto3.client('s3')
sns = boto3.client('sns')
sns_topic_arn = os.environ['SNS_TOPIC_ARN']
def lambda_handler(event, context):
try:
bucket = event['Records'][0]['s3']['bucket']['name']
key = event['Records'][0]['s3']['object']['key']
response = s3_client.get_object(Bucket=bucket, Key=key)
content = response['Body'].read().decode('utf-8').splitlines()
reader = csv.DictReader(content)
conn = psycopg2.connect(
host=os.environ['DB_HOST'],
database=os.environ['DB_NAME'],
user=os.environ['DB_USER'],
password=os.environ['DB_PASSWORD']
)
cur = conn.cursor()
for row in reader:
cur.execute("INSERT INTO customers (id, name, email) VALUES (%s, %s, %s)",
(row['id'], row['name'], row['email']))
conn.commit()
cur.close()
conn.close()
sns.publish(
TopicArn=sns_topic_arn,
Subject="Data Insertion Success",
Message=f"File {key} processed successfully and data inserted into RDS."
)
except Exception as e:
sns.publish(
TopicArn=sns_topic_arn,
Subject="Data Insertion Failed",
Message=str(e)
)
raise e