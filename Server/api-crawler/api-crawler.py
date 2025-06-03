import time
import json
import requests
import logging
import schedule
from configparser import ConfigParser
from datetime import datetime, timezone
from jsonschema import validate, ValidationError
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

class ISSCrawler:
    def __init__(self, api_url:str, schema_filepath:str):
        self.api_url = api_url
        self.json_schema = self.load_json_schema(schema_filepath)

    def load_json_schema(self, schema_filepath: str) -> dict:
        with open(schema_filepath, 'r') as file:
            return json.load(file)

    def api_response_to_json(self, response_content:str) -> dict | None:
        try:
            json_content = json.loads(response_content)
            logging.info(f"API response content is: {json_content}")
            return json_content
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON response: {e}")
            return None

    def validate_json(self, json_content:dict) -> bool:
        try:
            validate(instance=json_content, schema=self.json_schema)
            logging.info("API response is valid according to the JSON schema.")
            return True
        except ValidationError as e:
            logging.error(f"JSON schema validation failed: {e}")
            return False

    def check_api_response_code(self, response_code:int) -> bool:
        if response_code == 200:
            logging.info(f"API request was successful with status code: {response_code}")
            return True
        else:
            logging.error(f"API request failed with status code: {response_code}")
            return False

    def check_json_message_success(self, json_content:dict) -> bool:
        if json_content["message"] == "success":
            logging.info(f"API response message is '{json_content['message']}'.")
            return True
        else:
            logging.error(f"API response message is not 'success' but instead: {json_content.get('message')}")
            return False

    def convert_string_to_float(self, value:str) -> float | None:
        try:
            return float(value)
        except ValueError as e:
            logging.error(f"Failed to convert string to float: {e}")
            return None
        
    def convert_timestamp_for_influxdb(self, timestamp:int) -> datetime:
        return datetime.fromtimestamp(timestamp, timezone.utc)
    
    def convert_json_content(self, json_content:dict) -> dict:
        json_content["timestamp"] = self.convert_timestamp_for_influxdb(json_content["timestamp"])
        json_content["iss_position"]["latitude"] = self.convert_string_to_float(json_content["iss_position"]["latitude"])
        json_content["iss_position"]["longitude"] = self.convert_string_to_float(json_content["iss_position"]["longitude"])
        return json_content
        
    def fetch_iss_data(self) -> dict | None:
        try:
            response = requests.get(self.api_url)
        except Exception as e:
            logging.error(f"Failed to fetch ISS data from API: {e}")
            return None

        if self.check_api_response_code(response.status_code):
            json_content = self.api_response_to_json(response.content)

            if json_content is not None and self.validate_json(json_content) and self.check_json_message_success(json_content):
                json_content = self.convert_json_content(json_content)

                if json_content["iss_position"]["latitude"] is not None and json_content["iss_position"]["longitude"] is not None:
                    logging.info("Successfully fetched, validated and converted ISS data.")
                    return json_content
                else:
                    return None
            else:
                return None
        else:
            return None

class InfluxDBWriter:
    def __init__(self, url:str, token:str, org:str, ca_cert:str):
        self.client = InfluxDBClient(url=url, token=token, org=org, ssl_ca_cert=ca_cert, verify_ssl=True)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.org = org

    def write_data(self, measurement:str, bucket:str, source:str, latitude:float, longitude:float, timestamp:datetime):
        point = (
            Point(measurement)
            .tag("source", source)
            .field("latitude", latitude)
            .field("longitude", longitude)
            .time(timestamp)
        )
        logging.info(f"Final data to be written to InfluxDB: {point}")
        try:
            self.write_api.write(bucket=bucket, org=self.org, record=point)
            logging.info("Data successfully written to InfluxDB.")
        except Exception as e:
            logging.error(f"Failed to write data to InfluxDB: {e}")

class Main:
    def __init__(self):
        self.read_config()
        self.setup_logging()
        self.create_iss_crawler_instance()
        self.create_influxdb_writer_instance()

    def read_config(self):
        self.config = ConfigParser()
        self.config.read('config.ini')
    
    def setup_logging(self):
        logging.basicConfig(
            filename=self.config.get('logging', 'log_file'),
            level=logging.INFO,
            format='%(asctime)s %(levelname)s: %(message)s'
        )
        logging.info("Logging setup complete")

    def create_iss_crawler_instance(self):
        self.iss_crawler = ISSCrawler(
            self.config.get('iss-api', 'url'), 
            self.config.get('iss-api', 'schema_filepath')
        )
        logging.info("ISS Crawler instance created.")
        
    def create_influxdb_writer_instance(self):
        self.influx_writer = InfluxDBWriter(
            url=self.config.get('influxdb', 'url'),
            token=self.config.get('influxdb', 'token'),
            org=self.config.get('influxdb', 'org'),
            ca_cert=self.config.get('influxdb', 'ca_cert')
        )
        logging.info("InfluxDB Writer instance created.")

    def fetch_and_store_iss_data(self):
        logging.info("Wakeup time: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        try_counter = 2
        while try_counter > 0:
            iss_data = self.iss_crawler.fetch_iss_data()
            if iss_data is not None:
                try_counter = 0
                self.influx_writer.write_data(
                    measurement=self.config.get('influxdb', 'measurement'),
                    bucket=self.config.get('influxdb', 'bucket'),
                    source=self.config.get('iss-api', 'source'),
                    latitude=iss_data["iss_position"]["latitude"],
                    longitude=iss_data["iss_position"]["longitude"],
                    timestamp=iss_data["timestamp"]
                )
            else:
                try_counter -= 1
                logging.error("Failed to fetch or validate ISS data.")

if __name__ == "__main__":
    main = Main()
    logging.info("Starting ISS data crawler.")
    schedule.every().minute.at(":00").do(main.fetch_and_store_iss_data)
    while True:
        schedule.run_pending()
        time.sleep(1)