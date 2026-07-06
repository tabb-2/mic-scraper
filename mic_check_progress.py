"""Читает checkpoint из S3, выводит прогресс. Если всё готово — exit(0) пропускает следующий шаг."""
import boto3, json, os, sys

s3 = boto3.client(
    "s3",
    endpoint_url="https://storage.yandexcloud.net",
    aws_access_key_id=os.environ["YC_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["YC_SECRET_ACCESS_KEY"],
    region_name="ru-central1",
)
try:
    cp = json.loads(s3.get_object(Bucket="tebiz-data", Key="parsed/mic/checkpoint.json")["Body"].read())
    cats = json.loads(s3.get_object(Bucket="tebiz-data", Key="parsed/mic/categories.json")["Body"].read())
    done = len(cp.get("done_cats", []))
    total = len(cats)
    print(f"Progress: {done}/{total} categories done, {total - done} remaining")
    if done >= total:
        print("::notice::All categories complete! Skipping run.")
        sys.exit(0)
except Exception as e:
    print(f"Could not read progress: {e}")
