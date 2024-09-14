import io
import os
import re
import json
import csv
import tempfile
from celery import Celery
from celery.utils.log import get_task_logger
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


app = Celery('tasks', broker=os.getenv("CELERY_BROKER_URL"))
logger = get_task_logger(__name__)


@app.task
def process_csv_task(self, csv_data, prompt_template, file_name):
    try:
        # Convert CSV data to a file-like object using StringIO
        # csv_bytes = io.BytesIO(bytes(csv_data, 'utf-8'))
        # csv_input = io.TextIOWrapper(csv_bytes, encoding='utf-8')
        # csv_reader = csv.DictReader(csv_input)

        csv_input = io.StringIO(csv_data)
        csv_reader = csv.DictReader(csv_input)

        # Check if fieldnames are available
        if not csv_reader.fieldnames:
            print("CSV data is missing headers.")
            return {"error": "CSV data is missing headers."}

        # Prepare output temporary file
        with tempfile.NamedTemporaryFile(mode='w+', newline='', delete=False, encoding='utf-8') as tmp_output:
            fieldnames = csv_reader.fieldnames + ['Limpio']
            csv_writer = csv.DictWriter(tmp_output, fieldnames=fieldnames)
            csv_writer.writeheader()

            # Process each row
            for row in csv_reader:
                if not isinstance(row, dict):
                    print(f"Expected row to be dict, but got {type(row)}: {row}")
                    continue  # Skip or handle the error as needed

                row['Limpio'] = evaluate_lead(row, prompt_template)
                csv_writer.writerow(row)

            tmp_output.flush()
            tmp_output.seek(0)

            # Upload to Google Drive
            with open(tmp_output.name, 'rb') as f:
                file_link = upload_to_google_drive(f, file_name)

        # Clean up temporary file
        os.unlink(tmp_output.name)

        return {"message": "File processed and uploaded", "file_link": file_link}
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"error": str(e)}

def evaluate_lead(row, prompt_template):
    # Import openai here as well if needed
    import openai

    position_key = next((key for key in row.keys() if re.match(r"(?i)title", key)), None)
    position = row.get(position_key, "Unknown Position")
    prompt = prompt_template.replace("{{1.col6}}", position)

    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=1,
        temperature=1
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
        'parents': ['1zduCLHS7qG8GmUhCCuwABz5T2W1QVbKq']  # Update with your folder ID
    }

    media = MediaIoBaseUpload(file_stream, mimetype='text/csv')

    # Upload file
    file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

    # Make the file public
    drive_service.permissions().create(
        fileId=file['id'],
        body={'type': 'anyone', 'role': 'reader'}
    ).execute()

    # Return the public link
    file_link = f"https://drive.google.com/file/d/{file['id']}/view?usp=sharing"
    return file_link
