import os
import asyncio
from flask import Flask, render_template
import google.auth

from typing import List, Dict, Any

import send_message_chat
import bigquery_client

_, project = google.auth.default()
app = Flask(__name__)

def format_week_list_to_send_message(results: List[Dict[str, Any]]) -> str:
    return "\n".join([f"{i+1}. ({item['date']}) {item['term']}" for i, item in enumerate(results)])

def format_day_list_to_send_message(results: List[Dict[str, Any]]) -> str:
    return "\n".join([f"{i+1}. {item['term']}" for i, item in enumerate(results)])

@app.route("/")
def hello_world():
    return render_template("hello_world.html")

@app.route("/apiv1/top_terms_of_turkiye", methods=["GET"])
def apiv1_top_terms_of_turkiye():
    try:
        result = bigquery_client.get_top_terms_of_turkiye()
        return {'success': 'true', 'data': result}
    except Exception as e:
        return {'success': 'false', 'error': str(e)}
    
@app.route("/top_terms_of_turkiye", methods=["GET"])
def top_terms_of_turkiye():
    try:
        top_terms_of_week = bigquery_client.get_top_terms_of_week_turkiye()
        top_terms_of_yesterday = bigquery_client.get_top_terms_of_yesterday_turkiye()
        return render_template("top_terms_of_turkiye.html", 
                               top_terms_of_week=top_terms_of_week, 
                               top_terms_of_yesterday=top_terms_of_yesterday)
    except Exception as e:
        return {'success': 'false', 'error': str(e)}

@app.route("/send_message/top_terms_of_turkiye", methods=["GET"])
def send_message_top_terms_of_turkiye():
    try:
        top_terms_of_week = format_week_list_to_send_message(bigquery_client.get_top_terms_of_week_turkiye())
        top_terms_of_yesterday = format_day_list_to_send_message(bigquery_client.get_top_terms_of_yesterday_turkiye())
        message = "Top Terms of Week:\n" + top_terms_of_week + "\n\nTop Terms of Yesterday:\n" + top_terms_of_yesterday
        asyncio.run(send_message_chat.send_message(message=message))
        return {'success': 'true', 'data': 'Message sent'}
    except Exception as e:
        asyncio.run(send_message_chat.send_message(f"Error: get_top_terms_of_turkiye {str(e)}"))
        return {'success': 'false', 'error': str(e)}

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))