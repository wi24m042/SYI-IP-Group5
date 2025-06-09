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
        # Save the API URL in the class context
        self.api_url = api_url
        # Load the JSON schema from the specified file with the load_json_schema method and save it in the class context
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
        # Open the schema file 
        with open(schema_filepath, 'r') as file:
            # Load the file content as JSON schema and return it
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
        # Try to decode the response content as JSON
        try:
            # Parse the response content into a JSON object
            json_content = json.loads(response_content)
            # Log the JSON content for debugging purposes
            logging.info(f"API response content is: {json_content}")
            # Return the JSON content
            return json_content
        # Handle JSON decoding errors
        except json.JSONDecodeError as e:
            # Log the error message when decoding fails
            logging.error(f"Failed to decode JSON response: {e}")
            # Return None when decoding fails
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
        # Try to validate the JSON content against the loaded schema
        try:
            # Validate the JSON content against the loaded schema
            validate(instance=json_content, schema=self.json_schema)
            # Log success message if validation passes
            logging.info("API response is valid according to the JSON schema.")
            # Return True if validation is successful
            return True
        # Handle validation errors
        except ValidationError as e:
            # Log the error message when validation fails
            logging.error(f"JSON schema validation failed: {e}")
            # Return False when validation fails
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
        # Check if the response code is 200
        if response_code == 200:
            # Log success message if the response code is 200
            logging.info(f"API request was successful with status code: {response_code}")
            # Return True if the response code is 200
            return True
        # If the response code is not 200
        else:
            # Log error message if the response code is not 200
            logging.error(f"API request failed with status code: {response_code}")
            # Return False if the response code is not 200
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
        # Check if the message in the JSON content is "success"
        if json_content["message"] == "success":
            # Log success message if the API response message is "success"
            logging.info(f"API response message is '{json_content['message']}'.")
            # Return True if the message is "success"
            return True
        # If the message is not "success"
        else:
            # Log error message if the API response message is not "success"
            logging.error(f"API response message is not 'success' but instead: {json_content.get('message')}")
            # Return False if the message is not "success"
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
        # Try to convert the string value to a float
        try:
            # Convert the string value to a float and return it
            return float(value)
        # Handle ValueError if the string cannot be converted to a float
        except ValueError as e:
            # Log the error message when conversion fails
            logging.error(f"Failed to convert string to float: {e}")
            # Return None when conversion fails
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
        # Convert the timestamp from seconds since epoch to a datetime object in UTC
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
        # Use the convert_timestamp_for_influxdb method to convert the timestamp
        json_content["timestamp"] = self.convert_timestamp_for_influxdb(json_content["timestamp"])
        # Use the convert_string_to_float method to convert latitude to float
        json_content["iss_position"]["latitude"] = self.convert_string_to_float(json_content["iss_position"]["latitude"])
        # Use the convert_string_to_float method to convert longitude to float
        json_content["iss_position"]["longitude"] = self.convert_string_to_float(json_content["iss_position"]["longitude"])
        # Return the modified JSON content
        return json_content
        
    def fetch_iss_data(self) -> dict | None:
        """
        Function to fetch ISS data from the API, validate it, and convert it to the desired format.

        Parameters:
            self

        Returns:
            json_content (dict | None): The fetched and converted ISS data as a JSON object or None if any step fails.
        """
        # Try to fetch data from the ISS API
        try:
            # Make a GET request to the Open Notify API URL with a timeout of 15 seconds
            response = requests.get(self.api_url, timeout=15)
        # Handle any exceptions that occur during the request
        except Exception as e:
            # Log the error message when the request fails
            logging.error(f"Failed to fetch ISS data from API: {e}")
            # Return None when the request fails
            return None

        # Check if the API response code is valid
        if self.check_api_response_code(response.status_code):
            # Convert the API response content to JSON format using the api_response_to_json method
            json_content = self.api_response_to_json(response.content)
            # Check if the JSON content is valid according to the schema with the validate_json method and if the message indicates success with the check_json_message_success method
            if json_content is not None and self.validate_json(json_content) and self.check_json_message_success(json_content):
                # Convert the JSON content to the desired format for InfluxDB using the convert_json_content method
                json_content = self.convert_json_content(json_content)
                # Check if latitude and longitude are not None
                if json_content["iss_position"]["latitude"] is not None and json_content["iss_position"]["longitude"] is not None:
                    # Log the final JSON content to be written to InfluxDB
                    logging.info("Successfully fetched, validated and converted ISS data.")
                    # Return the final JSON content
                    return json_content
                # If latitude or longitude is None
                else:
                    # Return None when latitude or longitude is None
                    return None
            # If the JSON content is not valid or the message does not indicate success
            else:
                # Return None when json validation fails or message is not success
                return None
        # If the API response code is not 200
        else:
            # Return None when API response code is not 200
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
        # Create an InfluxDB client with the provided URL, token, organization, and CA certificate and save it in the class context
        self.client = InfluxDBClient(url=url, token=token, org=org, ssl_ca_cert=ca_cert, verify_ssl=True)
        # Create a write API instance with synchronous write options and save it in the class context
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        # Save the organization name in the class context
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
        # Create a Point object with the measurement name, tags, fields, and timestamp
        point = (
            Point(measurement)
            .tag("source", source)
            .field("latitude", latitude)
            .field("longitude", longitude)
            .time(timestamp)
        )
        # Log the final data point to be written to InfluxDB
        logging.info(f"Final data to be written to InfluxDB: {point}")
        # Try to write the data point to InfluxDB
        try:
            # Write the data point to the specified bucket and organization with the write API instance
            self.write_api.write(bucket=bucket, org=self.org, record=point)
            # Log success message when data is successfully written to InfluxDB
            logging.info("Data successfully written to InfluxDB.")
        # Handle any exceptions that occur during the write operation
        except Exception as e:
            # Log the error message when writing to InfluxDB fails
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
        # Initialize the configuration with the read_config method
        self.read_config()
        # Set up logging with the setup_logging method
        self.setup_logging()
        # Create an instance of the ISSCrawler class with the create_iss_crawler_instance method
        self.create_iss_crawler_instance()
        # Create an instance of the InfluxDBWriter class with the create_influxdb_writer_instance method
        self.create_influxdb_writer_instance()

    def read_config(self):
        """
        Function to read the configuration from the config.ini file.

        Parameters:
            self
        """
        # Create a ConfigParser instance
        self.config = ConfigParser()
        # Read the configuration file
        self.config.read('config.ini')
    
    def setup_logging(self):
        """
        Function to set up logging configuration.

        Parameters:
            self
        """
        # Set up logging configuration with the specified log file, level, and format
        logging.basicConfig(
            filename=self.config.get('logging', 'log_file'),
            level=logging.INFO,
            format='%(asctime)s %(levelname)s: %(message)s'
        )
        # Log welcome message with the current time
        logging.info(f"Welcome to the Position History Service (PHS) API Crawler! - the current time is {datetime.now()}")
        # Log completion of the logging setup
        logging.info("Logging setup complete")

    def create_iss_crawler_instance(self):
        """
        Function to create an instance of the ISSCrawler class.

        Parameters:
            self
        """
        # Create an instance of the ISSCrawler class with the API URL and schema file path from the configuration and save it in the class context
        self.iss_crawler = ISSCrawler(
            self.config.get('iss-api', 'url'), 
            self.config.get('iss-api', 'schema_filepath')
        )
        # Log the creation of the ISSCrawler instance
        logging.info("ISS Crawler instance created.")
        
    def create_influxdb_writer_instance(self):
        """
        Function to create an instance of the InfluxDBWriter class.

        Parameters:
            self
        """
        # Create an instance of the InfluxDBWriter class with the URL, token, organization, and CA certificate from the configuration and save it in the class context
        self.influx_writer = InfluxDBWriter(
            url=self.config.get('influxdb', 'url'),
            token=self.config.get('influxdb', 'token'),
            org=self.config.get('influxdb', 'org'),
            ca_cert=self.config.get('influxdb', 'ca_cert')
        )
        # Log the creation of the InfluxDBWriter instance
        logging.info("InfluxDB Writer instance created.")

    def fetch_and_store_iss_data(self):
        """
        Function to fetch ISS data and store it in InfluxDB.

        Parameters:
            self
        """
        # Log the wakeup time for the scheduled task
        logging.info("Wakeup time: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        # Initialize a counter for tries
        try_counter = 2
        # Loop until the data is successfully fetched or the try counter variable reaches zero
        while try_counter > 0:
            # Fetch ISS data using the fetch_iss_data method of the ISSCrawler instance
            iss_data = self.iss_crawler.fetch_iss_data()
            # If the ISS data is successfully fetched, validated and converted
            if iss_data is not None:
                # Set the try counter to zero to exit the loop
                try_counter = 0
                # Use the write_data method of the InfluxDBWriter instance to write the data to InfluxDB by passing the measurement, bucket, source, latitude, longitude, and timestamp
                self.influx_writer.write_data(
                    measurement=self.config.get('influxdb', 'measurement'),
                    bucket=self.config.get('influxdb', 'bucket'),
                    source=self.config.get('iss-api', 'source'),
                    latitude=iss_data["iss_position"]["latitude"],
                    longitude=iss_data["iss_position"]["longitude"],
                    timestamp=iss_data["timestamp"]
                )
            # If the ISS data is not successfully fetched, validated or converted
            else:
                # Decrement the try counter variable by 1
                try_counter -= 1
                # Log the failed attempt
                logging.error("Failed to fetch or validate ISS data.")

if __name__ == "__main__":
    # Set up the main function to run the API crawler
    main = Main()
    # Schedule the fetch_and_store_iss_data method to run every minute at the start of the minute
    schedule.every().minute.at(":00").do(main.fetch_and_store_iss_data)
    # While loop to keep the script running and check for scheduled tasks
    while True:
        # Run any pending scheduled tasks
        schedule.run_pending()
        # Sleep for 1 second to avoid high CPU usage
        time.sleep(1)