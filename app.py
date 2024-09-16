import os
from flask import Flask, flash, render_template, jsonify, redirect, request
from tasks import process_csv_task

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', "super-secret")


@app.route('/')
def main():
    return render_template('main.html')


@app.route('/limpiar', methods=['POST'])
def clean_lead_list():
    # Get the prompt from the form data
    prompt_template = request.form.get('prompt')
    
    # Get the uploaded file
    file = request.files.get('file')
    
    if not prompt_template:
        return jsonify({"error": "No prompt provided"}), 400

    if not file:
        return jsonify({"error": "No CSV file provided"}), 400

    # Read the file content
    try:
        # Read the file content and decode it
        csv_data = file.read().decode('utf-8')  # Adjust the encoding if needed
    except UnicodeDecodeError:
        return jsonify({"error": "Failed to decode file. Please ensure it's UTF-8 encoded."}), 400

    file_name = file.filename

    print(f"Received csv_data: {csv_data}")

    # Enqueue the task
    task = process_csv_task.delay(csv_data, prompt_template, file_name)
    return jsonify({"message": "File processing started", "task_id": task.id}), 200
