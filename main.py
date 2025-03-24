import os
import sys, json, logging
import asyncio
from flask import Flask, render_template
import google.auth

import send_message_chat
import bigquery_client

_, project = google.auth.default()
app = Flask(__name__)

@app.route("/")
def hello_world():
    asyncio.run(send_message_chat.send_message("Hello, World!"))
    return render_template("hello_world.html")

@app.route("/apiv1/top_terms_of_turkiye", methods=["GET"])
def apiv1_top_terms_of_turkiye():
    try:
        result = bigquery_client.get_top_terms_of_turkiye()
        asyncio.run(send_message_chat.send_message("Run: /apiv1/top_terms_of_turkiye"))
        return {'success': 'true', 'data': result}
    except Exception as e:
        asyncio.run(send_message_chat.send_message(f"Error: /apiv1/top_terms_of_turkiye {str(e)}"))
        return {'success': 'false', 'error': str(e)}
    
@app.route("/top_terms_of_turkiye", methods=["GET"])
def top_terms_of_turkiye():
    try:
        top_terms_of_week = bigquery_client.get_top_terms_of_week_turkiye()
        top_terms_of_yesterday = bigquery_client.get_top_terms_of_yesterday_turkiye()
        asyncio.run(send_message_chat.send_message("Run: get_top_terms_of_turkiye"))
        return render_template("top_terms_of_turkiye.html", 
                               top_terms_of_week=top_terms_of_week, 
                               top_terms_of_yesterday=top_terms_of_yesterday)
    except Exception as e:
        asyncio.run(send_message_chat.send_message(f"Error: get_top_terms_of_turkiye {str(e)}"))
        return {'success': 'false', 'error': str(e)}

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))