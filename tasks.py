import io
import os
import re
import json
import csv
import tempfile
import smtplib
import time
import requests  # Para hacer consultas DNS
import dns.resolver  # Para resolver registros MX
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from celery import Celery
from celery.utils.log import get_task_logger
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import google.generativeai as genai
import google.api_core.exceptions

app = Celery('tasks', broker=os.getenv("CELERY_BROKER_URL"))
logger = get_task_logger(__name__)

# --- Configure Gemini API ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    logger.error("GEMINI_API_KEY environment variable not set.")
else:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        logger.info("Google Generative AI configured successfully.")
    except Exception as e:
        logger.error(f"Failed to configure Google Generative AI: {e}")
# ---------------------------

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

def mx_lookup(domain):
    """
    Looks up MX records for a domain and determines the email provider.
    Returns "Gmail", "Outlook", or "Other".
    """
    try:
        # Método 1: Usar Google DNS API (similar al script original)
        url = f"https://dns.google.com/resolve?name={domain}&type=MX"
        
        try:
            response = requests.get(url, timeout=5)
            if response.status_code != 200:
                logger.warning(f"Google DNS API error for {domain}: {response.status_code}")
                # Si hay error, intentaremos con el método alternativo
                raise Exception("Google DNS API error")
                
            data = response.json()
            
            if not data.get("Answer"):
                return "No MX Records"
                
            email_provider = ""
            
            for answer in data.get("Answer", []):
                mx_raw = answer.get("data", "").lower()
                
                if any(provider in mx_raw for provider in ["google.com", "googlemail.com", "gmail.com"]):
                    email_provider = "Gmail"
                elif any(provider in mx_raw for provider in ["outlook.com", "hotmail.com", "microsoft.com", "office365.com"]):
                    email_provider = "Outlook"
                    break  # Outlook tiene precedencia
            
            if not email_provider:
                email_provider = "Other"
                
            return email_provider
            
        except Exception as e:
            logger.warning(f"Error using Google DNS API for {domain}: {e}, trying alternative method")
            # Si el método de Google DNS falla, intentamos con dns.resolver
            
        # Método 2: Usar dns.resolver (alternativa si el método 1 falla)
        try:
            answers = dns.resolver.resolve(domain, 'MX')
            
            if not answers:
                return "No MX Records"
                
            email_provider = ""
            
            for rdata in answers:
                mx = str(rdata.exchange).lower()
                
                if any(provider in mx for provider in ["google.com", "googlemail.com", "gmail.com"]):
                    email_provider = "Gmail"
                elif any(provider in mx for provider in ["outlook.com", "hotmail.com", "microsoft.com", "office365.com"]):
                    email_provider = "Outlook"
                    break  # Outlook tiene precedencia
            
            if not email_provider:
                email_provider = "Other"
                
            return email_provider
            
        except Exception as e:
            logger.error(f"DNS resolver error for {domain}: {e}")
            return "ERROR"
            
    except Exception as e:
        logger.error(f"MX lookup error for {domain}: {e}")
        return "ERROR"

@app.task
def process_csv_task(csv_data, prompt_template, file_name):
    try:
        csv_input = io.StringIO(csv_data)
        csv_reader = csv.DictReader(csv_input)

        if not csv_reader.fieldnames:
            logger.error("CSV data is missing headers.")
            return {"error": "CSV data is missing headers."}

        with tempfile.NamedTemporaryFile(mode='w+', newline='', delete=False, encoding='utf-8') as tmp_output:
            # Añadir columna MX Result
            fieldnames = csv_reader.fieldnames + ['Limpio', 'Bundle', 'MX Result']
            csv_writer = csv.DictWriter(tmp_output, fieldnames=fieldnames)
            csv_writer.writeheader()

            company_counts = {}

            for row_index, row in enumerate(csv_reader):
                if not isinstance(row, dict):
                    logger.warning(f"Row {row_index+1}: Expected row to be dict, but got {type(row)}: {row}")
                    continue

                # Obtener compañía para agrupar
                company_key = next((key for key in row.keys() if re.match(r"(?i)(organization name|company|company name|organization)", key)), None)
                company_name = row.get(company_key, "Unknown Company")

                company_counts.setdefault(company_name, {'count': 0})
                company_counts[company_name]['count'] += 1
                employee_number = company_counts[company_name]['count']

                bundle_number = (employee_number - 1) // 50 + 1
                row['Bundle'] = bundle_number

                # Procesar MX para determinar el proveedor de correo
                # Primero, buscar la columna de email
                email_key = next((key for key in row.keys() if re.match(r"(?i)email", key)), None)
                
                if email_key and row.get(email_key):
                    email = row.get(email_key).strip()
                    if '@' in email:
                        try:
                            domain = email.split('@')[1]
                            # Añadir un pequeño retraso para evitar ser limitado por la API
                            time.sleep(0.2)
                            mx_result = mx_lookup(domain)
                            row['MX Result'] = mx_result
                        except Exception as e:
                            logger.error(f"Error processing MX for email {email}: {str(e)}")
                            row['MX Result'] = "ERROR"
                    else:
                        row['MX Result'] = "Invalid Email"
                else:
                    row['MX Result'] = "No Email"

                # Evaluar si el lead es "limpio"
                try:
                    row['Limpio'] = evaluate_lead_with_retry(row, prompt_template)
                except Exception as e:
                    logger.error(f"Failed to evaluate lead for row {row_index+1} ({company_name}) after retries: {str(e)}")
                    row['Limpio'] = "Error"

                csv_writer.writerow(row)

            tmp_output.flush()
            tmp_output.seek(0)
            output_file_path = tmp_output.name

        # Upload after the 'with' block ensures the file is closed
        with open(output_file_path, 'rb') as f:
            file_link = upload_to_google_drive(f, file_name)

        os.unlink(output_file_path)

        subject = "File Processing Complete"
        body = f"The file '{file_name}' has been processed and uploaded.\nYou can access it here: {file_link}"
        send_email(subject, body, ["tomas.manguel@theleadgenerationguys.com", "abramson@theleadgenerationguys.com"])

        return {"message": "File processed and uploaded", "file_link": file_link}
    except Exception as e:
        logger.exception(f"Error processing file '{file_name}': {str(e)}")

        subject = "Error Processing File"
        body = f"An error occurred while processing the file '{file_name}': {str(e)}"
        send_email(subject, body, "tomas.manguel@theleadgenerationguys.com")

        return {"error": f"Processing failed: {str(e)}"}


def evaluate_lead_with_retry(row, prompt_template, retries=3, delay=5):
    """Attempts to evaluate the lead using Gemini, with retries on specific errors."""
    for attempt in range(retries):
        try:
            return evaluate_lead(row, prompt_template)
        # Catch specific retryable Google API errors (like timeouts)
        except google.api_core.exceptions.DeadlineExceeded as e:
            logger.warning(f"Gemini API timeout (DeadlineExceeded) occurred on attempt {attempt + 1}/{retries} for row {row}. Retrying in {delay}s...")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                logger.error(f"Gemini API timeout failed after {retries} attempts for row {row}.")
                raise e # Re-raise the last exception if all retries fail
        # Catch other potentially retryable errors (e.g., temporary server issues)
        except google.api_core.exceptions.RetryError as e:
             logger.warning(f"Gemini API retryable error occurred on attempt {attempt + 1}/{retries} for row {row}. Retrying in {delay}s...")
             if attempt < retries - 1:
                 time.sleep(delay)
             else:
                 logger.error(f"Gemini API retryable error failed after {retries} attempts for row {row}.")
                 raise e
        # Catch API key issues or configuration problems - generally not retryable
        except (google.api_core.exceptions.PermissionDenied, google.api_core.exceptions.Unauthenticated) as e:
            logger.error(f"Gemini API authentication/permission error: {str(e)}. Check API key and permissions.")
            raise e # Not retryable, raise immediately
        except Exception as e:
            # Catch any other unexpected errors during the API call
            logger.error(f"Unexpected error during Gemini lead evaluation (attempt {attempt + 1}/{retries}) for row {row}: {str(e)}")
            if attempt < retries - 1:
                logger.info(f"Retrying in {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"Unexpected error failed after {retries} attempts for row {row}.")
                raise e # Re-raise after final attempt

def evaluate_lead(row, prompt_template):
    """Evaluates a single lead using the Gemini API."""

    position_key = next((key for key in row.keys() if re.match(r"(?i)title", key)), None)
    position = row.get(position_key, "Unknown Position")
    prompt = prompt_template.replace("[POSICION]", position)

    try:
        # Initialize the Gemini model - consider initializing once outside the function if performance is critical
        model = genai.GenerativeModel('gemini-1.5-flash')  # Cambiado a 1.5-flash por disponibilidad

        # Define generation configuration
        generation_config = genai.GenerationConfig(
            max_output_tokens=1,
            temperature=1.0 # Ensure it's a float
        )

        # Call the Gemini API
        response = model.generate_content(
            prompt,
            generation_config=generation_config,
            request_options={'timeout': 60} # Set timeout for the request
        )

        # --- Process the response ---
        # Check for blocking first
        if hasattr(response, 'prompt_feedback') and response.prompt_feedback and hasattr(response.prompt_feedback, 'block_reason'):
            block_reason = response.prompt_feedback.block_reason
            logger.warning(f"Gemini prompt blocked for position '{position}'. Reason: {block_reason}")
            return f"Blocked ({block_reason})" # Return specific block info

        # Obtenemos el texto directamente si está disponible
        if hasattr(response, 'text'):
            result_text = response.text.strip()
            return result_text
        
        # Alternativa si text no está disponible
        if hasattr(response, 'candidates') and response.candidates and hasattr(response.candidates[0], 'content'):
            parts = response.candidates[0].content.parts
            if parts:
                result_text = str(parts[0]).strip()
                return result_text

        # Si llegamos aquí, no pudimos obtener una respuesta válida
        logger.warning(f"No se pudo extraer texto de la respuesta para '{position}'")
        return "No Content"

    # Catch specific API errors that might occur during the call itself
    except google.api_core.exceptions.GoogleAPIError as e:
        logger.error(f"Gemini API Error evaluating position '{position}': {str(e)}")
        raise e # Re-raise to be handled by the retry mechanism
    except Exception as e:
        # Catch any other unexpected error during API call or response processing
        logger.error(f"Unexpected error in evaluate_lead for position '{position}': {str(e)}")
        raise e # Re-raise to be handled by the retry mechanism


def upload_to_google_drive(file_stream, file_name):
    SCOPES = ['https://www.googleapis.com/auth/drive.file']
    SERVICE_ACCOUNT_INFO = os.environ.get('GOOGLE_SERVICE_ACCOUNT_INFO')

    if not SERVICE_ACCOUNT_INFO:
        # Use logger and raise a specific error
        logger.error("Google service account info not found in environment variables")
        raise ValueError("Google service account info not found in environment variables")

    try:
        service_account_info = json.loads(SERVICE_ACCOUNT_INFO)
        creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        drive_service = build('drive', 'v3', credentials=creds)

        file_metadata = {
            'name': file_name,
            # Consider making the parent folder ID configurable
            'parents': ['1zduCLHS7qG8GmUhCCuwABz5T2W1QVbKq']
        }

        # Ensure the file stream is suitable for MediaIoBaseUpload (needs read method)
        media = MediaIoBaseUpload(file_stream, mimetype='text/csv', resumable=True) # Added resumable=True for robustness

        logger.info(f"Uploading '{file_name}' to Google Drive...")
        request = drive_service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink') # Get webViewLink directly
        
        response = None
        upload_start_time = time.time()
        while response is None:
            status, response = request.next_chunk()
            if status:
                logger.info(f"Upload progress: {int(status.progress() * 100)}%")
        
        upload_duration = time.time() - upload_start_time
        logger.info(f"File '{file_name}' uploaded successfully in {upload_duration:.2f} seconds. File ID: {response['id']}")

        # Make the file publicly readable (consider if this is always desired)
        logger.info(f"Setting permissions for File ID: {response['id']} to anyone with the link.")
        drive_service.permissions().create(
            fileId=response['id'],
            body={'type': 'anyone', 'role': 'reader'}
        ).execute()

        # Use the webViewLink directly if available, otherwise construct it
        file_link = response.get('webViewLink', f"https://drive.google.com/file/d/{response['id']}/view?usp=sharing")
        return file_link

    except json.JSONDecodeError as e:
        logger.error(f"Error decoding Google service account JSON: {str(e)}")
        raise ValueError(f"Invalid Google service account JSON: {str(e)}")
    except Exception as e:
        # Catch errors during Drive API interaction
        logger.exception(f"Error uploading file '{file_name}' to Google Drive: {str(e)}")
        raise IOError(f"Google Drive upload failed: {str(e)}") # Raise IOError or a custom exception