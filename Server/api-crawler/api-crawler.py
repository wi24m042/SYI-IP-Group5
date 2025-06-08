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
    """
    Class to handle fetching, validating, and converting ISS data from the API.
    """

    def __init__(self, api_url:str, schema_filepath:str):
        """
        Constructor to initialize the ISSCrawler class.

        Parameters:
            self
            api_url (str): The URL of the ISS API to fetch data from.
            schema_filepath (str): Path to the JSON schema file for validation.
        """
        self.api_url = api_url
        self.json_schema = self.load_json_schema(schema_filepath)

    def load_json_schema(self, schema_filepath: str) -> dict:
        """
        Function to load the JSON schema from a file.

        Parameters:
            self
            schema_filepath (str): Path to the JSON schema file.

        Returns:
            json_schema (dict): The loaded JSON schema.
        """
        with open(schema_filepath, 'r') as file:
            return json.load(file)

    def api_response_to_json(self, response_content:str) -> dict | None:
        """ 
        Function to convert API response content to JSON format.

        Parameters:
            self
            response_content (str): The content of the API response.

        Returns:
            json_content (dict | None): The API response content as a JSON object or None if decoding fails.
        """
        try:
            json_content = json.loads(response_content)
            logging.info(f"API response content is: {json_content}")
            return json_content
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON response: {e}")
            return None

    def validate_json(self, json_content:dict) -> bool:
        """
        Function to validate the JSON content against the predefined schema.

        Parameters:
            self
            json_content (dict): The JSON content to validate.

        Returns:
            validation_result (bool): True if the JSON content is valid according to the schema, False otherwise.
        """
        try:
            validate(instance=json_content, schema=self.json_schema)
            logging.info("API response is valid according to the JSON schema.")
            return True
        except ValidationError as e:
            logging.error(f"JSON schema validation failed: {e}")
            return False

    def check_api_response_code(self, response_code:int) -> bool:
        """
        Function to check the API response code.

        Parameters:
            self
            response_code (int): The HTTP status code from the API response.

        Returns:
            expected_response_code (bool): True if the response code indicates success (200), False otherwise.
        """
        if response_code == 200:
            logging.info(f"API request was successful with status code: {response_code}")
            return True
        else:
            logging.error(f"API request failed with status code: {response_code}")
            return False

    def check_json_message_success(self, json_content:dict) -> bool:
        """
        Function to check if the API response message indicates success.

        Parameters:
            self
            json_content (dict): The JSON content from the API response.

        Returns:
            message_success (bool): True if the message indicates success, False otherwise.
        """
        if json_content["message"] == "success":
            logging.info(f"API response message is '{json_content['message']}'.")
            return True
        else:
            logging.error(f"API response message is not 'success' but instead: {json_content.get('message')}")
            return False

    def convert_string_to_float(self, value:str) -> float | None:
        """
        Function to convert a string to a float.

        Parameters:
            self
            value (str): The string value to convert.

        Returns:
            result_float (float | None): The converted float value or None if conversion fails.
        """
        try:
            return float(value)
        except ValueError as e:
            logging.error(f"Failed to convert string to float: {e}")
            return None
        
    def convert_timestamp_for_influxdb(self, timestamp:int) -> datetime:
        """
        Function to convert a timestamp to a datetime object in UTC.

        Parameters:
            self
            timestamp (int): The timestamp to convert.

        Returns:
            datetime (datetime): The converted datetime object in UTC.
        """
        return datetime.fromtimestamp(timestamp, timezone.utc)
    
    def convert_json_content(self, json_content:dict) -> dict:
        """
        Function to convert the JSON content to the desired format for InfluxDB.

        Parameters:
            self
            json_content (dict): The JSON content to convert.

        Returns:
            json_content (dict): The converted JSON content with timestamp and coordinates in the correct format.
        """
        json_content["timestamp"] = self.convert_timestamp_for_influxdb(json_content["timestamp"])
        json_content["iss_position"]["latitude"] = self.convert_string_to_float(json_content["iss_position"]["latitude"])
        json_content["iss_position"]["longitude"] = self.convert_string_to_float(json_content["iss_position"]["longitude"])
        return json_content
        
    def fetch_iss_data(self) -> dict | None:
        """
        Function to fetch ISS data from the API, validate it, and convert it to the desired format.

        Parameters:
            self

        Returns:
            json_content (dict | None): The fetched and converted ISS data as a JSON object or None if any step fails.
        """
        try:
            response = requests.get(self.api_url, timeout=15)
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
    """
    Class to handle writing data to InfluxDB.
    """

    def __init__(self, url:str, token:str, org:str, ca_cert:str):
        """
        Constructor to initialize the InfluxDBWriter class.

        Parameters:
            self
            url (str): The URL of the InfluxDB instance.
            token (str): The authentication token for InfluxDB.
            org (str): The organization name in InfluxDB.
            ca_cert (str): Path to the CA certificate for SSL verification.
        """
        self.client = InfluxDBClient(url=url, token=token, org=org, ssl_ca_cert=ca_cert, verify_ssl=True)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.org = org

    def write_data(self, measurement:str, bucket:str, source:str, latitude:float, longitude:float, timestamp:datetime):
        """
        Function to write data to InfluxDB.

        Parameters:
            self
            measurement (str): The measurement name for the data point.
            bucket (str): The InfluxDB bucket to write the data to.
            source (str): The source of the data.
            latitude (float): The latitude value to write.
            longitude (float): The longitude value to write.
            timestamp (datetime): The timestamp for the data point.
        """
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
    """
    Main class to orchestrate the ISS data fetching and writing to InfluxDB.
    """

    def __init__(self):
        """
        Constructor to initialize the Main class and set up the necessary components.

        Parameters:
            self
        """
        self.read_config()
        self.setup_logging()
        self.create_iss_crawler_instance()
        self.create_influxdb_writer_instance()

    def read_config(self):
        """
        Function to read the configuration from the config.ini file.

        Parameters:
            self
        """
        self.config = ConfigParser()
        self.config.read('config.ini')
    
    def setup_logging(self):
        """
        Function to set up logging configuration.

        Parameters:
            self
        """
        logging.basicConfig(
            filename=self.config.get('logging', 'log_file'),
            level=logging.INFO,
            format='%(asctime)s %(levelname)s: %(message)s'
        )
        logging.info("Logging setup complete")

    def create_iss_crawler_instance(self):
        """
        Function to create an instance of the ISSCrawler class.

        Parameters:
            self
        """
        self.iss_crawler = ISSCrawler(
            self.config.get('iss-api', 'url'), 
            self.config.get('iss-api', 'schema_filepath')
        )
        logging.info("ISS Crawler instance created.")
        
    def create_influxdb_writer_instance(self):
        """
        Function to create an instance of the InfluxDBWriter class.

        Parameters:
            self
        """
        self.influx_writer = InfluxDBWriter(
            url=self.config.get('influxdb', 'url'),
            token=self.config.get('influxdb', 'token'),
            org=self.config.get('influxdb', 'org'),
            ca_cert=self.config.get('influxdb', 'ca_cert')
        )
        logging.info("InfluxDB Writer instance created.")

    def fetch_and_store_iss_data(self):
        """
        Function to fetch ISS data and store it in InfluxDB.

        Parameters:
            self
        """
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