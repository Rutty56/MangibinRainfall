import os
import requests
import xml.etree.ElementTree as ET
import csv
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from linebot import LineBotApi
from linebot.models import TextSendMessage
import schedule
import time
from dotenv import load_dotenv

load_dotenv()
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
FOLDER_ID = os.getenv("FOLDER_ID")
SCOPES = ['https://www.googleapis.com/auth/drive.file']
SERVICE_ACCOUNT_FILE = 'credentials.json'
line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)

def get_registered_users():
    try:
        with open("registered_users.txt", "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        return []

def fetch_weather_data():
    url = "https://data.tmd.go.th/api/WeatherToday/V2/?uid=api&ukey=api12345"
    response = requests.get(url)
    return response.content

def parse_and_save_csv(xml_data, filename):
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

def upload_to_drive(filename):
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
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
        line_bot_api.push_message(user_id, TextSendMessage(text=message))

def job():
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d_%H-%M")
    filename = f"weather_{timestamp}.csv"
    xml_data = fetch_weather_data()
    parse_and_save_csv(xml_data, filename)
    file_url = upload_to_drive(filename)
    message = f"⛅ รายงานสภาพอากาศประจำวันที่ {now.strftime('%Y-%m-%d %H:%M')}\n📁 ดาวน์โหลด CSV: {file_url}"
    send_to_registered_users(message)
    os.remove(filename)

schedule.every().day.at("08:00").do(job)
schedule.every().day.at("12:00").do(job)

print("LINE Bot Weather CSV Service is running...")
while True:
    schedule.run_pending()
    time.sleep(60)
