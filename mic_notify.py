"""Отправляет прогресс в Telegram после каждого run."""
import boto3, json, os, urllib.request, urllib.parse

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
    pct = round(done / total * 100, 1)
    filled = int(pct / 10)
    bar = "X" * filled + "." * (10 - filled)
    status = os.environ.get("JOB_STATUS", "unknown")
    icon = "OK" if status == "success" else "FAIL"
    if done >= total:
        msg = "MIC done! All categories scraped."
    else:
        msg = icon + " MIC [" + bar + "] " + str(pct) + "%\n" + str(done) + "/" + str(total) + " cats, left: " + str(total - done)
except Exception as e:
    msg = "MIC error: " + str(e)

token = os.environ["TG_BOT_TOKEN"]
chat_id = os.environ["TG_CHAT_ID"]
data = urllib.parse.urlencode({"chat_id": chat_id, "text": msg}).encode()
try:
    urllib.request.urlopen(
        urllib.request.Request("https://api.telegram.org/bot" + token + "/sendMessage", data=data)
    )
    print("TG sent:", msg)
except Exception as e:
    print("TG failed:", e)
