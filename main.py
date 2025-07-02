import json
import os
import time
import requests
import xml.etree.ElementTree as ET
import csv
from datetime import datetime, timedelta, timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from linebot import LineBotApi, WebhookHandler
from linebot.models import TextSendMessage, MessageEvent, TextMessage
from linebot.exceptions import InvalidSignatureError
from dotenv import load_dotenv
from flask import Flask, request, abort
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException

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

def fetch_weather_data_with_retry(retries=3, wait_seconds=3):
    url = "https://data.tmd.go.th/api/WeatherToday/V2/?uid=api&ukey=api12345"
    for attempt in range(retries):
        try:
            options = Options()
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")

            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(60)
            driver.get(url)
            time.sleep(30)  # ให้เวลา JS โหลด

            page_source = driver.page_source
            driver.quit()

            if "<Station" in page_source:
                return page_source.encode("utf-8")  # กลับเป็น bytes เพื่อใช้ต่อใน ET.fromstring
            else:
                print(f"Attempt {attempt+1}: ไม่พบ XML จาก TMD")
        except WebDriverException as e:
            print(f"Attempt {attempt+1}: Selenium error: {e}")
        if attempt < retries - 1:
            print(f"Retrying in {wait_seconds} seconds...")
            time.sleep(wait_seconds)
    raise Exception("Failed to fetch weather data after retries")

def fetch_weather_data():
    return fetch_weather_data_with_retry()

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

def parse_and_save_csv(xml_data, filename):
    try:
        root = ET.fromstring(xml_data)
        stations = root.findall(".//Station")
        all_fields = set()
        rows = []
        for station in stations:
            station_data = extract_all_fields(station)
            all_fields.update(station_data.keys())
            rows.append(station_data)

        priority_fields = ['WmoStationNumber', 'Observation_Rainfall', 'Observation_Rainfall_Unit']
        remaining_fields = sorted(f for f in all_fields if f not in priority_fields)
        fieldnames = priority_fields + remaining_fields

        with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
    except Exception as e:
        raise Exception(f"Error parsing XML data: {e}")

def upload_to_drive(filename):
    service = build('drive', 'v3', credentials=credentials)
    file_metadata = {'name': os.path.basename(filename), 'parents': [FOLDER_ID]}
    media = MediaFileUpload(filename, mimetype='text/csv')
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

def count_stations_in_weather_data():
    try:
        xml_data = fetch_weather_data()
        root = ET.fromstring(xml_data)
        stations = root.findall(".//Station")
        return len(stations)
    except Exception as e:
        print(f"Error counting stations: {e}")
        return None

def send_daily_weather_update():
    print("✅ Running scheduled weather update...")
    bangkok_tz = timezone(timedelta(hours=7))
    now = datetime.now(bangkok_tz)
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"weather_{timestamp}.csv"
    try:
        xml_data = fetch_weather_data()
        parse_and_save_csv(xml_data, filename)
        file_url = upload_to_drive(filename)
        message = f"🌤️ อัปเดตสภาพอากาศประจำวันที่ {now.strftime('%d/%m/%Y')} ครับ\n📂 ดาวน์โหลดไฟล์: {file_url}"
    except Exception as e:
        message = f"❌ ข้อผิดพลาดในการอัปเดตสภาพอากาศ: {e}"
    finally:
        if os.path.exists(filename):
            os.remove(filename)
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
    elif text == 'เช็คข้อมูล':
        count = count_stations_in_weather_data()
        reply = f"📡 ขณะนี้มีข้อมูลจากทั้งหมด {count} สถานีครับ" if count else "เกิดข้อผิดพลาดในการดึงข้อมูลสถานี 😢"
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
    elif text == 'ดึงข้อมูล':
        bangkok_tz = timezone(timedelta(hours=7))
        now = datetime.now(bangkok_tz)
        timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"weather_{timestamp}.csv"
        try:
            xml_data = fetch_weather_data()
            parse_and_save_csv(xml_data, filename)
            file_url = upload_to_drive(filename)
            reply = f"✅ ดึงข้อมูลเรียบร้อยแล้ว!\n📂 ดาวน์โหลดไฟล์ CSV ได้ที่: {file_url}"
        except Exception as e:
            reply = f"❌ เกิดข้อผิดพลาดในการดึงข้อมูล: {e}"
        finally:
            if os.path.exists(filename):
                os.remove(filename)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
    else:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(
            text="กรุณาพิมพ์คำสั่งที่ถูกต้อง เช่น 'สมัครรับบริการ', 'ยกเลิกสมัคร', 'เช็คข้อมูล', หรือ 'ดึงข้อมูล'"
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
    send_daily_weather_update()
    return "✅ Triggered weather update"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
