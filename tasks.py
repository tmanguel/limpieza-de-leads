import io
import os
import re
import json
import csv
import tempfile
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from celery import Celery
from celery.utils.log import get_task_logger
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import openai

app = Celery('tasks', broker=os.getenv("CELERY_BROKER_URL"))
logger = get_task_logger(__name__)

def send_email(subject, body, to_emails):
    EMAIL_HOST = os.environ.get('EMAIL_HOST')
    EMAIL_PORT = os.environ.get('EMAIL_PORT')
    EMAIL_HOST_USER = os.environ.get('EMAIL_HOST_USER')
    EMAIL_HOST_PASSWORD = os.environ.get('EMAIL_HOST_PASSWORD')
    EMAIL_USE_TLS = os.environ.get('EMAIL_USE_TLS', 'True') == 'True'

    if not all([EMAIL_HOST, EMAIL_PORT, EMAIL_HOST_USER, EMAIL_HOST_PASSWORD]):
        logger.error("Email credentials are not fully provided in environment variables")
        return

    if isinstance(to_emails, str):
        to_emails = [to_emails]

    if not to_emails:
        logger.error("Recipient emails not provided")
        return

    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_HOST_USER
        msg['To'] = ', '.join(to_emails)
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP(EMAIL_HOST, int(EMAIL_PORT))
        if EMAIL_USE_TLS:
            server.starttls()
        server.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
        server.send_message(msg)
        server.quit()
        logger.info("Email sent successfully")
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")

@app.task
def process_csv_task(csv_data, prompt_template, file_name):
    try:
        csv_input = io.StringIO(csv_data)
        csv_reader = csv.DictReader(csv_input)

        if not csv_reader.fieldnames:
            print("CSV data is missing headers.")
            return {"error": "CSV data is missing headers."}

        with tempfile.NamedTemporaryFile(mode='w+', newline='', delete=False, encoding='utf-8') as tmp_output:
            fieldnames = csv_reader.fieldnames + ['Limpio', 'Bundle']
            csv_writer = csv.DictWriter(tmp_output, fieldnames=fieldnames)
            csv_writer.writeheader()

            company_counts = {}

            for row in csv_reader:
                if not isinstance(row, dict):
                    print(f"Expected row to be dict, but got {type(row)}: {row}")
                    continue

                company_key = next((key for key in row.keys() if re.match(r"(?i)(organization name|company|company name|organization)", key)), None)
                company_name = row.get(company_key, "Unknown Company")

                company_counts.setdefault(company_name, {'count': 0})
                company_counts[company_name]['count'] += 1
                employee_number = company_counts[company_name]['count']

                bundle_number = (employee_number - 1) // 50 + 1
                row['Bundle'] = bundle_number

                # Add retry mechanism and error handling for evaluating lead
                try:
                    row['Limpio'] = evaluate_lead_with_retry(row, prompt_template)
                except Exception as e:
                    logger.error(f"Failed to evaluate lead for row {row}: {str(e)}")
                    row['Limpio'] = "Error"

                csv_writer.writerow(row)

            tmp_output.flush()
            tmp_output.seek(0)

            with open(tmp_output.name, 'rb') as f:
                file_link = upload_to_google_drive(f, file_name)

        os.unlink(tmp_output.name)

        subject = "File Processing Complete"
        body = f"The file '{file_name}' has been processed and uploaded.\nYou can access it here: {file_link}"
        send_email(subject, body, ["tomas.manguel@theleadgenerationguys.com", "abramson@theleadgenerationguys.com"])

        return {"message": "File processed and uploaded", "file_link": file_link}
    except Exception as e:
        error_message = f"Error processing file '{file_name}': {str(e)}"
        print(error_message)

        subject = "Error Processing File"
        body = error_message
        send_email(subject, body, "tomas.manguel@theleadgenerationguys.com")

        return {"error": str(e)}

def evaluate_lead_with_retry(row, prompt_template, retries=3, delay=5):
    for attempt in range(retries):
        try:
            return evaluate_lead(row, prompt_template)
        except openai.error.Timeout as e:
            logger.warning(f"Timeout occurred, retrying {attempt + 1}/{retries}...")
            if attempt < retries - 1:
                time.sleep(delay)  # Wait before retrying
            else:
                raise e
        except Exception as e:
            logger.error(f"Unexpected error during lead evaluation: {str(e)}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise e

def evaluate_lead(row, prompt_template):
    position_key = next((key for key in row.keys() if re.match(r"(?i)title", key)), None)
    position = row.get(position_key, "Unknown Position")
    prompt = prompt_template.replace("[POSICION]", position)

    response = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=1,
        temperature=1,
        timeout=60  # Set a reasonable timeout for each request
    )

    return response.choices[0].message['content'].strip()

def upload_to_google_drive(file_stream, file_name):
    SCOPES = ['https://www.googleapis.com/auth/drive.file']
    SERVICE_ACCOUNT_INFO = os.environ.get('GOOGLE_SERVICE_ACCOUNT_INFO')

    if not SERVICE_ACCOUNT_INFO:
        raise Exception("Google service account info not found in environment variables")

    service_account_info = json.loads(SERVICE_ACCOUNT_INFO)
    creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=creds)

    file_metadata = {
        'name': file_name,
        'parents': ['1zduCLHS7qG8GmUhCCuwABz5T2W1QVbKq']
    }

    media = MediaIoBaseUpload(file_stream, mimetype='text/csv')

    file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

    drive_service.permissions().create(
        fileId=file['id'],
        body={'type': 'anyone', 'role': 'reader'}
    ).execute()

    file_link = f"https://drive.google.com/file/d/{file['id']}/view?usp=sharing"
    return file_link
