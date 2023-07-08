import json
import os
import subprocess
import tempfile
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

        # バイナリデータを取得
        bin = fetch_image_bin(message_id)

        if message_type == "video":
            with tempfile.NamedTemporaryFile(suffix=".jpg") as temp_preview_image:
                # プレビュー画像を作成する
                create_preview_image(bin, temp_preview_image)

                # プレビュー画像をS3にアップロード
                preview_filename = os.path.join("preview", message_id)
                preview_image_url = upload_s3(bin=temp_preview_image.read(), filename=preview_filename)

        # S3にアップロード
        original_filename = os.path.join("original", message_id)
        media_url = upload_s3(bin=bin, filename=original_filename)

        broadcast_messages.append(
            {
                "type": message_type,
                "originalContentUrl": media_url,
                "previewImageUrl": media_url if message_type == "image" else preview_image_url,
            }
        )

    # ブロードキャストする
    boardcast(messages=broadcast_messages)

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}


def fetch_image_bin(message_id: str) -> bytes:
    url = f"https://api-data.line.me/v2/bot/message/{message_id}/content"
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "Authorization": f"Bearer {os.environ['CHANNEL_ACCESS_TOKEN']}",
    }
    req = urllib.request.Request(url, method="GET", headers=headers)
    with urllib.request.urlopen(req) as res:
        return res.read()


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


def create_preview_image(video_bin: bytes, temp_preview_file: tempfile._TemporaryFileWrapper) -> None:
    with tempfile.NamedTemporaryFile(suffix=".mp4") as temp_video:
        temp_video.write(video_bin)
        temp_video.seek(0)

        # ffmpegコマンドの実行
        command = [
            "ffmpeg",
            "-y",
            "-ss",
            "0",
            "-i",
            temp_video.name,
            "-vframes",
            "1",
            "-q:v",
            "2",
            temp_preview_file.name,
        ]
        subprocess.call(command)

        temp_preview_file.seek(0)
