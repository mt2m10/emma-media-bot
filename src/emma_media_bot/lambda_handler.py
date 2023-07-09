import json
import mimetypes
import os
import urllib.request

import boto3


def lambda_handler(event, context):
    print(json.dumps(event))

    broadcast_messages = []
    for message_event in json.loads(event["body"])["events"]:
        message_type = message_event["message"]["type"]

        # 画像・動画以外は受け付けない
        if message_type not in ["image", "video"]:
            continue

        message_id = message_event["message"]["id"]

        media_url = create_public_url(message_id=message_id)
        preview_image_url = create_public_url(message_id=message_id, type="preview")

        broadcast_messages.append(
            {
                "type": message_type,
                "originalContentUrl": media_url,
                "previewImageUrl": preview_image_url,
            }
        )

    # ブロードキャストする
    boardcast(messages=broadcast_messages)

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}


def create_public_url(message_id: str, type: str = "original") -> str:
    if type not in ["original", "preview"]:
        raise ValueError(f"unsupported type: {type}")

    # 画像・動画ファイルを取得する
    fetch_func = fetch_content if type == "original" else fetch_preview_content
    content, content_type = fetch_func(message_id=message_id)

    # S3にアップロード
    ext = mimetypes.guess_extension(content_type)
    if ext is None:
        ext = ""

    upload_path = os.path.join(type, f"{message_id}{ext}")
    public_url = upload_s3(bin=content, filename=upload_path)

    return public_url


def fetch_content(message_id: str) -> tuple[bytes, str]:
    url = f"https://api-data.line.me/v2/bot/message/{message_id}/content"
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "Authorization": f"Bearer {os.environ['CHANNEL_ACCESS_TOKEN']}",
    }
    req = urllib.request.Request(url, method="GET", headers=headers)
    with urllib.request.urlopen(req) as res:
        headers = res.info()
        return res.read(), headers["Content-Type"]


def fetch_preview_content(message_id: str) -> tuple[bytes, str]:
    url = f"https://api-data.line.me/v2/bot/message/{message_id}/content/preview"
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "Authorization": f"Bearer {os.environ['CHANNEL_ACCESS_TOKEN']}",
    }
    req = urllib.request.Request(url, method="GET", headers=headers)
    with urllib.request.urlopen(req) as res:
        headers = res.info()
        return res.read(), headers["Content-Type"]


def upload_s3(bin: bytes, filename: str) -> str:
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(os.environ["S3_MEDIA_BUCKET"])
    bucket.put_object(Body=bin, Key=filename)
    media_url = os.path.join(os.environ["S3_MEDIA_DOMAIN"], filename)
    return media_url


def boardcast(messages: list) -> None:
    if messages == []:
        return

    url = "https://api.line.me/v2/bot/message/broadcast"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f'Bearer {os.environ["CHANNEL_ACCESS_TOKEN"]}',
    }
    body = {
        "messages": messages,
    }
    req = urllib.request.Request(url, data=json.dumps(body).encode("utf-8"), method="POST", headers=headers)
    urllib.request.urlopen(req)
