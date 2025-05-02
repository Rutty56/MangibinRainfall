import json
import os
import requests
import xml.etree.ElementTree as ET
import csv
import folium
from folium.plugins import MarkerCluster
from datetime import datetime, timedelta, timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from linebot import LineBotApi, WebhookHandler
from linebot.models import TextSendMessage, MessageEvent, TextMessage
from linebot.exceptions import InvalidSignatureError
from dotenv import load_dotenv
from flask import Flask, request, abort

load_dotenv()

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET")
FOLDER_ID = os.getenv("FOLDER_ID")
WEATHER_TRIGGER_KEY = os.getenv("WEATHER_TRIGGER_KEY")
SCOPES = ['https://www.googleapis.com/auth/drive.file']
REGISTERED_USER_FILE = "registered_users.txt"

SERVICE_ACCOUNT_INFO = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
if SERVICE_ACCOUNT_INFO is None:
    raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON is not set in the environment variables")

credentials = service_account.Credentials.from_service_account_info(
    json.loads(SERVICE_ACCOUNT_INFO), scopes=SCOPES)

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

app = Flask(__name__)

def get_registered_users():
    if not os.path.exists(REGISTERED_USER_FILE):
        return []
    with open(REGISTERED_USER_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def register_user(user_id):
    users = get_registered_users()
    if user_id not in users:
        with open(REGISTERED_USER_FILE, "a", encoding="utf-8") as f:
            f.write(f"{user_id}\n")
        print(f"User {user_id} registered.")
    else:
        print(f"User {user_id} already registered.")

def unregister_user(user_id):
    users = get_registered_users()
    if user_id in users:
        users.remove(user_id)
        with open(REGISTERED_USER_FILE, "w", encoding="utf-8") as f:
            f.writelines(f"{uid}\n" for uid in users)
        print(f"User {user_id} unregistered.")

def fetch_weather_data():
    url = "https://data.tmd.go.th/api/WeatherToday/V2/?uid=api&ukey=api12345"
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        raise Exception(f"Error fetching weather data: {response.status_code}")

def extract_all_fields(elem, prefix=""):
    data = {}
    for child in elem:
        tag = child.tag
        key_prefix = f"{prefix}{tag}" if prefix == "" else f"{prefix}_{tag}"
        for attr_key, attr_val in child.attrib.items():
            data[f"{key_prefix}_{attr_key}"] = attr_val
        if child.text and child.text.strip():
            data[key_prefix] = child.text.strip()
        data.update(extract_all_fields(child, key_prefix))
    return data

def create_rainfall_map(xml_data):
    try:
        map_center = [13.7563, 100.5018]
        m = folium.Map(location=map_center, zoom_start=7)

        marker_cluster = MarkerCluster().add_to(m)

        root = ET.fromstring(xml_data)
        stations = root.findall(".//Station")

        for station in stations:
            data = extract_all_fields(station)
            station_name = data.get("StationNameThai", "ไม่ทราบสถานี")
            latitude = float(data.get("Latitude", 0))
            longitude = float(data.get("Longitude", 0))
            rainfall = float(data.get("Observation_Rainfall", 0))

            color = "blue"
            if rainfall > 100:
                color = "red"
            elif rainfall > 50:
                color = "orange"
            elif rainfall > 20:
                color = "green"

            folium.CircleMarker(
                location=[latitude, longitude],
                radius=8,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.6,
                popup=f"{station_name} - {rainfall} มม."
            ).add_to(marker_cluster)

        map_filename = "rainfall_map.html"
        m.save(map_filename)
        print(f"แผนที่ฝนถูกบันทึกเป็นไฟล์: {map_filename}")

    except Exception as e:
        print(f"เกิดข้อผิดพลาดในการสร้างแผนที่ฝน: {e}")

def upload_to_drive(filename):
    service = build('drive', 'v3', credentials=credentials)
    file_metadata = {'name': os.path.basename(filename), 'parents': [FOLDER_ID]}
    media = MediaFileUpload(filename, mimetype='text/html')
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    file_id = file.get('id')
    service.permissions().create(fileId=file_id, body={"type": "anyone", "role": "reader"}).execute()
    return f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"

def send_to_registered_users(message):
    user_ids = get_registered_users()
    if not user_ids:
        print("⚠️ No registered users.")
    for user_id in user_ids:
        try:
            line_bot_api.push_message(user_id, TextSendMessage(text=message))
        except Exception as e:
            print(f"Error sending message to {user_id}: {e}")

def generate_rainfall_map():
    try:
        xml_data = fetch_weather_data() 
        create_rainfall_map(xml_data) 
        upload_map_to_drive()  
    except Exception as e:
        print(f"เกิดข้อผิดพลาดในการสร้างแผนที่ฝน: {e}")

def upload_map_to_drive():
    map_filename = "rainfall_map.html"
    file_url = upload_to_drive(map_filename) 
    message = f"📍 ดูแผนที่ฝนที่นี่: {file_url}"
    send_to_registered_users(message) 

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_id = event.source.user_id
    text = event.message.text.strip()

    if text == 'สมัครรับบริการ':
        register_user(user_id)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="คุณได้สมัครขอรับบริการแล้ว! ✅"))
    elif text == 'ยกเลิกสมัคร':
        unregister_user(user_id)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="คุณได้ยกเลิกการรับบริการแล้ว 😢"))
    elif text == 'แผนที่':
        try:
            generate_rainfall_map() 
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="แผนที่ถูกสร้างเรียบร้อยแล้ว!"))
        except Exception as e:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ เกิดข้อผิดพลาด: {e}"))
    else:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(
            text="กรุณาพิมพ์คำสั่งที่ถูกต้อง เช่น 'สมัครรับบริการ', 'ยกเลิกสมัคร', หรือ 'แผนที่'"
        ))

@app.route("/", methods=["GET"])
def health_check():
    return "LINE Bot is running."

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

@app.route("/trigger-weather", methods=["GET"])
def trigger_weather():
    if request.args.get("key") != WEATHER_TRIGGER_KEY:
        return "❌ Unauthorized", 403
    generate_rainfall_map()
    return "✅ Triggered weather update"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
