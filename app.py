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
    data = request.json
    prompt_template = data.get("prompt")
    csv_data = data.get("file", {}).get("data")
    file_name = data.get("file", {}).get("name")    
    
    print(f"Received csv_data: {csv_data}")
    
    if csv_data:
        task = process_csv_task.delay(csv_data, prompt_template, file_name)
        return jsonify({"message": "File processing started", "task_id": task.id}), 200
    else:
        return jsonify({"error": "No CSV data provided"}), 400
