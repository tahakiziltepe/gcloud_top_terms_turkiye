import os
import sys, json, logging
import asyncio
from flask import Flask, request, render_template
import google.auth

import send_message_chat
import bigquery_client

class JsonFormatter(logging.Formatter):
    def format(self, record):
        json_log_object = {
            "severity": record.levelname,
            "message": record.getMessage(),
        }
        json_log_object.update(getattr(record, "json_fields", {}))
        return json.dumps(json_log_object)
logger = logging.getLogger(__name__)
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(JsonFormatter())
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)

_, project = google.auth.default()
app = Flask(__name__)

@app.route("/")
def hello_world():
    asyncio.run(send_message_chat.send_message("Hello, World!"))
    return "Hello, World!"

@app.route("/apiv1/top_terms_of_turkiye", methods=["GET"])
def apiv1_top_terms_of_turkiye():
    try:
        result = bigquery_client.get_top_terms_of_turkiye()
        asyncio.run(send_message_chat.send_message("Run: /apiv1/top_terms_of_turkiye"))
        return {'success': 'true', 'data': [dict(row) for row in result]}
    except Exception as e:
        asyncio.run(send_message_chat.send_message("Error: /apiv1/top_terms_of_turkiye"))
        return {'success': 'false', 'error': str(e)}
    
@app.route("/top_terms_of_turkiye", methods=["GET"])
def top_terms_of_turkiye():
    try:
        top_terms_of_week = bigquery_client.get_top_terms_of_week_turkiye()
        top_terms_of_yesterday = bigquery_client.get_top_terms_of_yesterday_turkiye()
        asyncio.run(send_message_chat.send_message("Run: get_top_terms_of_turkiye"))
        top_terms_of_week_list = [dict(row) for row in top_terms_of_week]
        top_terms_of_yesterday_list = [dict(row) for row in top_terms_of_yesterday]
        return render_template("top_terms_of_turkiye.html", 
                               top_terms_of_week=top_terms_of_week_list, 
                               top_terms_of_yesterday=top_terms_of_yesterday_list)
    except Exception as e:
        asyncio.run(send_message_chat.send_message("Error: get_top_terms_of_turkiye"))
        return {'success': 'false', 'error': str(e)}



if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))