from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from flask import Flask
import time, re, os, io
from threading import Thread
import pdfplumber
import pandas as pd
from twilio.rest import Client

# === Configuration ===
SERVICE_ACCOUNT_FILE = 'service_account.json'
FOLDER_ID = '1huS2WYMk_dcx3wsc5WxubOxM0y71tSUO'
DOWNLOAD_DIR = 'downloads'
LOCAL_FILENAME = 'latest_manifest.pdf'
DRIVER_EXCEL_PATH = 'driver_info.xlsx'

# Twilio credentials
TWILIO_ACCOUNT_SID = 'your_twilio_account_sid'
TWILIO_AUTH_TOKEN = 'your_twilio_auth_token'
TWILIO_WHATSAPP_NUMBER = 'whatsapp:+14155238886'

app = Flask(__name__)

# === Google Drive Service Setup ===
def get_drive_service():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=['https://www.googleapis.com/auth/drive']
    )
    return build('drive', 'v3', credentials=credentials)

# === Download PDF from Drive ===
def download_file_from_drive(service, file_id, file_name):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    destination_path = os.path.join(DOWNLOAD_DIR, file_name)
    request = service.files().get_media(fileId=file_id)
    with io.FileIO(destination_path, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}%")
    print(f"Downloaded: {destination_path}")
    return destination_path

# === Extract Shipment Records ===
def extract_shipments_from_pdf(pdf_path):
    shipments = []
    with pdfplumber.open(pdf_path) as pdf:
        lines = []
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                lines.extend(text.splitlines())

    i = 0
    while i < len(lines):
        line = lines[i]
        if line.endswith("Pieces/Wt (KG)"):
            try:
                name = line.replace("Pieces/Wt (KG)", "").strip()
                barcode_line = lines[i + 1].strip()
                barcode_match = re.search(r"\d{12}", barcode_line)
                barcode = barcode_match.group(0) if barcode_match else "N/A"
                address_part = barcode_line.split(barcode)[-1].strip()

                next_line = lines[i + 2].strip()
                if '@' in next_line:
                    address = address_part
                    city_postal_line = lines[i + 3].strip()
                    phone_cod_line = lines[i + 4].strip()
                    cod_amount_line = lines[i + 5].strip()
                    i += 6
                else:
                    address = f"{address_part} {next_line}"
                    city_postal_line = lines[i + 4].strip()
                    phone_cod_line = lines[i + 5].strip()
                    cod_amount_line = lines[i + 6].strip()
                    i += 7

                postal_match = re.search(r",\s*(\d{5})", city_postal_line)
                postal_code = postal_match.group(1) if postal_match else "Unknown"

                phone_match = re.search(r"\b5\d{7}\b", phone_cod_line)
                phone = phone_match.group(0) if phone_match else "Unknown"

                cod_match = re.search(r"([\d,.]+)", cod_amount_line)
                cod = cod_match.group(1) if cod_match else "0"

                shipments.append({
                    "name": name,
                    "address": address,
                    "postal_code": postal_code,
                    "phone": phone,
                    "cod": cod,
                    "barcode": barcode
                })
            except IndexError:
                i += 1
        else:
            i += 1
    return shipments

# === Check Driver in Excel ===
def driver_exists(driver_name):
    df = pd.read_excel(DRIVER_EXCEL_PATH)
    return driver_name.strip().lower() in df['Driver Name'].str.strip().str.lower().values

# === Send WhatsApp message via Twilio ===
def send_whatsapp_message(to_number, message):
    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    try:
        client.messages.create(
            from_=TWILIO_WHATSAPP_NUMBER,
            body=message,
            to=f'whatsapp:+230{to_number}'
        )
        print(f"✅ Message sent to {to_number}")
    except Exception as e:
        print(f"❌ Error sending to {to_number}: {e}")

# === Monitor Drive Folder ===
def monitor_folder(service):
    print("🚀 Monitoring latest PDF in Google Drive folder...")
    last_processed_id = None

    while True:
        try:
            response = service.files().list(
                q=f"'{FOLDER_ID}' in parents and mimeType='application/pdf'",
                orderBy='modifiedTime desc',
                pageSize=1,
                fields='files(id, name, modifiedTime)'
            ).execute()

            files = response.get('files', [])
            if files:
                latest_file = files[0]
                file_id = latest_file['id']
                file_name = latest_file['name']
                driver_name_match = re.match(r'manifest_(.+)\.pdf', file_name, re.IGNORECASE)
                if file_id != last_processed_id and driver_name_match:
                    driver_name = driver_name_match.group(1).replace('_', ' ').strip()
                    if driver_exists(driver_name):
                        local_path = download_file_from_drive(service, file_id, LOCAL_FILENAME)
                        shipments = extract_shipments_from_pdf(local_path)

                        for s in shipments:
                            message = (f"Your order {s.get('barcode', 'N/A')} is ready for delivery at {s['address']} "
                                       f"with the driver {driver_name}. Please keep this amount mention Rs {s['cod']} ready. "
                                       "If paid, please send screenshot. Thank you!")
                            send_whatsapp_message(s['phone'], message)
                    else:
                        print(f"❌ Driver '{driver_name}' not found in Excel.")
                    last_processed_id = file_id
            time.sleep(15)
        except Exception as error:
            print(f"❌ Error: {error}")
            time.sleep(60)

if __name__ == '__main__':
    drive_service = get_drive_service()
    monitor_thread = Thread(target=monitor_folder, args=(drive_service,), daemon=True)
    monitor_thread.start()
    app.run(port=5000)
