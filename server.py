from flask import Flask, jsonify, request, abort
from functools import wraps
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest
from google.oauth2 import service_account
import logging
from flask_cors import CORS
from datetime import datetime
from google.analytics.admin import AnalyticsAdminServiceClient

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)
CORS(app)

PROPERTY_ID = 'properties/437018419'
KEY_FILE_PATH = 'dellenhauerprod.json'
API_KEY = 'd422b131-c67b-4e0c-86fc-3b5f4b9adc36'

def require_api_key(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if request.headers.get('x-api-key') == API_KEY:
            return f(*args, **kwargs)
        else:
            logging.warning('Unauthorized access attempt.')
            abort(401)
    return decorated_function

def fetch_data_from_analytics(event_name, start_date, end_date):
    try:
        credentials = service_account.Credentials.from_service_account_file(KEY_FILE_PATH)
        client = BetaAnalyticsDataClient(credentials=credentials)
        
        dimension_name = "customEvent:article_id"
        if event_name == 'channel_join':
            dimension_name = "customEvent:channel_id"
        
        request = RunReportRequest(
            property=PROPERTY_ID,
            dimensions=[{"name": dimension_name}],
            metrics=[{"name": "eventCount"}],
            date_ranges=[{"start_date": start_date, "end_date": end_date}],
            dimension_filter={
                "filter": {
                    "field_name": "eventName",
                    "string_filter": {
                        "value": event_name
                    }
                }
            },
            order_bys=[{"metric": {"metric_name": "eventCount"}, "desc": True}],
        )
        response = client.run_report(request)
        return process_response(response)
    except Exception as e:
        logging.error(f"Error fetching data from analytics: {e}")
        raise e

def process_response(response):
    data = []
    for row in response.rows:
        dimension_value = row.dimension_values[0].value if row.dimension_values else "Unknown"
        metric_value = row.metric_values[0].value if row.metric_values else "Unknown"
        data.append({"id": dimension_value, "count": metric_value})
    return data

@app.route('/')
def home():
    return 'Google Analytics Console'

def get_date_range():
    start_date = request.args.get('start_date', '2022-01-01')
    end_date = request.args.get('end_date', datetime.today().strftime('%Y-%m-%d'))
    logging.debug(f"Date range: start_date={start_date}, end_date={end_date}")
    return start_date, end_date

@app.route('/analytics/<event_name>', methods=['GET'])
@require_api_key
def get_analytics_data(event_name):
    try:
        start_date, end_date = get_date_range()
        data = fetch_data_from_analytics(event_name, start_date, end_date)
        return jsonify(data)
    except Exception as e:
        logging.error(f"Error fetching '{event_name}' data: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
