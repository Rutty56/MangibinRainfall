import json
import os
import requests
import xml.etree.ElementTree as ET
import csv
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from linebot import LineBotApi, WebhookHandler
from linebot.models import TextSendMessage, MessageEvent, TextMessage
from linebot.exceptions import InvalidSignatureError
from dotenv import load_dotenv
from flask import Flask, request, abort

# โหลด environment variables
load_dotenv()
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET")
FOLDER_ID = os.getenv("FOLDER_ID")
SCOPES = ['https://www.googleapis.com/auth/drive.file']

SERVICE_ACCOUNT_INFO = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

if SERVICE_ACCOUNT_INFO is None:
    raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON is not set in the environment variables")

credentials = service_account.Credentials.from_service_account_info(
    json.loads(SERVICE_ACCOUNT_INFO), scopes=SCOPES)

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

app = Flask(__name__)

def get_registered_users():
    user_ids = os.getenv("REGISTERED_USER_IDS", "").split(",") if os.getenv("REGISTERED_USER_IDS") else []
    return user_ids

def fetch_weather_data():
    url = "https://data.tmd.go.th/api/WeatherToday/V2/?uid=api&ukey=api12345"
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        raise Exception(f"Error fetching weather data: {response.status_code}")

def parse_and_save_csv(xml_data, filename):
    try:
        root = ET.fromstring(xml_data)
        stations = root.findall(".//Station")
        all_tags = set()
        for station in stations:
            for elem in station:
                all_tags.add(elem.tag)
        fieldnames = sorted(list(all_tags))

        with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for station in stations:
                row = {tag: station.findtext(tag) for tag in fieldnames}
                writer.writerow(row)
    except Exception as e:
        raise Exception(f"Error parsing XML data: {e}")

def upload_to_drive(filename):
    service = build('drive', 'v3', credentials=credentials)
    file_metadata = {
        'name': os.path.basename(filename),
        'parents': [FOLDER_ID]
    }
    media = MediaFileUpload(filename, mimetype='text/csv')
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    file_id = file.get('id')
    service.permissions().create(
        fileId=file_id,
        body={"type": "anyone", "role": "reader"},
    ).execute()
    return f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"

def send_to_registered_users(message):
    user_ids = get_registered_users()
    for user_id in user_ids:
        try:
            line_bot_api.push_message(user_id, TextSendMessage(text=message))
        except Exception as e:
            print(f"Error sending message to {user_id}: {e}")

def register_user(user_id):
    registered_users = get_registered_users()
    if user_id not in registered_users:
        registered_users.append(user_id)
        os.environ["REGISTERED_USER_IDS"] = ",".join(registered_users)
        print(f"User {user_id} has been registered.")
    else:
        print(f"User {user_id} is already registered.")

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_id = event.source.user_id
    text = event.message.text

    if text == 'สมัครขอรับบริการจ้า':
        register_user(user_id)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="คุณได้สมัครขอรับบริการแล้ว!")
        )

def job():
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d_%H-%M")
    filename = f"weather_{timestamp}.csv"
    try:
        xml_data = fetch_weather_data()
        parse_and_save_csv(xml_data, filename)
        file_url = upload_to_drive(filename)
        message = f"⛅ รายงานสภาพอากาศประจำวันที่ {now.strftime('%Y-%m-%d %H:%M')}\n📁 ดาวน์โหลด CSV: {file_url}"
        send_to_registered_users(message)
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        if os.path.exists(filename):
            os.remove(filename)

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

if __name__ == "__main__":
    app.run(host="0.0.0.0",debug=True, port=5000)
