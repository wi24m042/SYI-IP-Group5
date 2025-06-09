import ssl
import logging
import pandas as pd
from typing import Generator
from datetime import datetime
from configparser import ConfigParser
from influxdb_client import InfluxDBClient
from influxdb_client.client.flux_table import TableList
from aiohttp import web
import aiohttp_cors
from aiohttp_spyne import AIOSpyne
from spyne.protocol.soap import Soap12
from spyne import Application as SpyneApplication, rpc, ServiceBase, Iterable, Integer, Unicode, ComplexModel, Float

class PositionHistoryServiceApplication(SpyneApplication):
    """
    Custom Spyne application for the Position History Service.
    """

    def __init__(self, services:list, tns:str, in_protocol:Soap12, out_protocol:Soap12, influx_reader:'InfluxDBReader'):
        """
        Initializes the PositionHistoryServiceApplication as a custom SpyneApplication.

        Parameters:
            self
            services (list): List of services to be included in the application.
            tns (str): Target namespace for the application.
            in_protocol (Soap12): Input protocol for the application.
            out_protocol (Soap12): Output protocol for the application.
            influx_reader (InfluxDBReader): InfluxDB reader instance.
        """
        # Call the parent class constructor with the provided parameters tns, in_protocol, out_protocol
        super().__init__(services=services, tns=tns, in_protocol=in_protocol, out_protocol=out_protocol)
        # Store the InfluxDB reader instance for use in the class instance
        self.influx_reader = influx_reader

class LocationRecord(ComplexModel):
    """
    This custom Spyne ComplexModel represents a location record with timestamp, latitude, longitude, and source.
    """
    # Timestamp field in seconds since epoch as an integer
    timestamp = Integer
    # Latitude field as a float
    latitude = Float
    # Longitude field as a float
    longitude = Float
    # Source field as a unicode string
    source = Unicode

class PositionHistoryService(ServiceBase):
    """
    This service provides methods to retrieve location history and closest entries by timestamp.
    """
    
    @rpc(Integer, Integer, _returns=Iterable(LocationRecord))
    def GetLocationHistory(ctx, start_time:int, stop_time:int) -> Generator[LocationRecord, None, None] | None:
        """
        API function to retrieve location history records between the specified start and stop times from the InfluxDB.

        Parameters:
            ctx: The context of the request.
            start_time (int): The start time in seconds since epoch.
            stop_time (int): The stop time in seconds since epoch.

        Returns:
            json_list (Generator | None): A generator yielding LocationRecord objects or None if an error occurs.
        """
        # Log the received request for location history
        logging.info(f"Received request for location history between {start_time} and {stop_time}")
        # Retrieve data from InfluxDB using the provided start and stop times with the execute_read_request method of the InfluxDBReader instance
        table_list = ctx.app.influx_reader.execute_read_request(start_time=start_time, stop_time=stop_time)
        # Check if the table_list is not None
        if table_list is not None:
            # Iterate through the table list
            for table in table_list:
                # Iterate through the table records
                for record in table.records:
                    # Create a LocationRecord object with the timestamp, latitude, longitude, and source from the current record values
                    location_record = LocationRecord(
                        timestamp=int(record.get_time().timestamp()),
                        latitude=record.values.get("latitude"),
                        longitude=record.values.get("longitude"),
                        source=record.values.get("source")
                    )
                    # Log the yielded location record
                    logging.info(f"Yielding location record: {location_record}")
                    # Yield the created LocationRecord record
                    yield location_record
        # If the table_list is None
        else:
            # Log an error message indicating failure to retrieve data from InfluxDB
            logging.error("Failed to retrieve data from InfluxDB")
            # Yield None to indicate no records found
            yield None

    @rpc(Integer, _returns=LocationRecord)
    def GetClosestEntryByTimestamp(ctx, timestamp:int) -> LocationRecord | None:
        """
        API function to retrieve the closest location entry by timestamp from the InfluxDB.

        Parameters:
            ctx: The context of the request.
            timestamp (int): The timestamp in seconds since epoch.

        Returns:
            location_record (LocationRecord | None): The closest location record or None if not found.
        """
        # Log the received request for closest entry by timestamp
        logging.info(f"Received request for closest entry by timestamp {timestamp}")
        # Initialize start time for the query range
        start_time = timestamp - 60
        # Initialize stop time for the query range
        stop_time = timestamp + 60

        # Loop to query data until a valid record is found or the range is expanded beyond 1 hour
        while True:
            # Log the current query range
            logging.info(f"Querying data from {start_time} to {stop_time}")
            # Retrieve data from InfluxDB using the provided start and stop times with the execute_read_request method of the InfluxDBReader instance
            table_list = ctx.app.influx_reader.execute_read_request(start_time=start_time, stop_time=stop_time)
            # Check if the table_list is not None
            if table_list is not None:
                # Flatten the table_list into a list of dictionaries containing the values of each record
                data_points = [record.values for table in table_list for record in table.records]
                # Log the number of data points found in the current range
                logging.info(f"Found {len(data_points)} data points in the range {start_time} to {stop_time}")
                # If no data points are found, expand the range by 60 seconds in both directions
                if len(data_points) == 0:
                    # Update the start time to expand the range by another 60 seconds backwards
                    start_time -= 60
                    # Update the stop time to expand the range by another 60 seconds forwards
                    stop_time += 60
                    # If the start time is more than 1 hour before the timestamp, log a message and return None
                    if start_time <= timestamp - 3600:
                        # Log that the range has been expanded but still no data points found
                        logging.info(f"Expanded range to {start_time} to {stop_time}, but still no data points found")
                        # Return None to indicate no records found
                        return None
                    # Log the expanded range for the next iteration
                    logging.info(f"No data points found, expanding range to {start_time} to {stop_time}")
                # If data points are found
                else:
                    # Convert the list of data points into a pandas DataFrame
                    points_df = pd.DataFrame(data_points)
                    # Log the pandas DataFrame of data points
                    logging.info(f"Data points DataFrame:\n{points_df}")
                    # Update the '_time' column to pandas datetime format
                    points_df['_time'] = pd.to_datetime(points_df['_time'])
                    # Calculate the absolute time difference from the provided timestamp in seconds
                    points_df['delta'] = abs(points_df['_time'] - pd.to_datetime(timestamp, unit='s', utc=True))
                    # Export the DataFrame row with the minimum time delta as a pandas Series
                    closest = points_df.loc[points_df['delta'].idxmin()]
                    # Log the closest entry found
                    logging.info(f"Closest entry found: {closest}")
                    # Create a LocationRecord object with the closest entry's timestamp, latitude, longitude, and source - the timestamp is converted from pandas datetime to seconds since epoch
                    location_record = LocationRecord(
                        timestamp=int(closest.get("_time").timestamp()),
                        latitude=float(closest.get("latitude")),
                        longitude=float(closest.get("longitude")),
                        source=closest.get("source")
                    )
                    # Log the closest location record being returned
                    logging.info(f"Returning closest entry: {location_record}")
                    # Return the created LocationRecord record
                    return location_record
            # If the table_list is None
            else:
                # Log an error message indicating failure to retrieve data from InfluxDB
                logging.error("Failed to retrieve data from InfluxDB")
                # Return None to indicate no records found
                return None

class InfluxDBReader:
    """
        This class is responsible for querying the InfluxDB database for location data.
    """
    def __init__(self, url:str, token:str, org:str, bucket:str, measurement:str, source:str, ca_cert:str):
        """
        Initializes the InfluxDBReader with the necessary parameters to connect to the InfluxDB.

        Parameters:
            url (str): The URL of the InfluxDB instance.
            token (str): The authentication token for the InfluxDB.
            org (str): The organization name in the InfluxDB.
            bucket (str): The bucket name in the InfluxDB.
            measurement (str): The measurement name to query.
            source (str): The source of the data to filter by.
            ca_cert (str): Path to the CA certificate for SSL verification.
        """
        # Create an InfluxDB client with the provided URL, token, organization, and CA certificate and save it in the class context
        self.client = InfluxDBClient(url=url, token=token, org=org, ssl_ca_cert=ca_cert, verify_ssl=True)
        # Create a query API instance from the client and save it in the class context
        self.query_api = self.client.query_api()
        # Save the bucket name in the class context
        self.bucket = bucket
        # Save the organization name in the class context
        self.org = org
        # Save the measurement name in the class context
        self.measurement = measurement
        # Save the source of the data in the class context
        self.source = source

    def query_builder(self, start_time:int, stop_time:int) -> str:
        """
        Builds a Flux query string to retrieve location data from the InfluxDB.

        Parameters:
            start_time (int): The start time in seconds since epoch.
            stop_time (int): The stop time in seconds since epoch.

        Returns:    
            query (str): The Flux query as a string.
        """
        # Construct the Flux query string using the provided start and stop times, bucket, measurement, and source
        query = f'''
                from(bucket: "{self.bucket}")
                    |> range(start: {start_time}, stop: {stop_time})
                    |> filter(fn: (r) => r._measurement == "{self.measurement}")
                    |> filter(fn: (r) => r.source == "{self.source}")
                    |> filter(fn: (r) => r._field == "latitude" or r._field == "longitude")
                    |> pivot(
                        rowKey: ["_time"],
                        columnKey: ["_field"],
                        valueColumn: "_value"
                    )
                '''
        # Return the constructed query string
        return query
    
    def read_data(self, query:str) -> TableList:
        """
        Executes the provided Flux query against the InfluxDB and returns the results.

        Parameters:
            query (str): The Flux query to execute.

        Returns:
            table_list (TableList): The resulting entries of the query from the InfluxDB as a TableList.
        """
        # Try to execute the query using the query API
        try:
            # Execute the query with the query method of the query API instance and store the result in table_list variable
            table_list = self.query_api.query(org=self.org, query=query)
            # Log the successful execution of the query
            logging.info(f"Query executed successfully: {query}")
            # Return the resulting table list
            return table_list
        # If an exception occurs during the query execution
        except Exception as e:
            # Log the error message indicating failure to execute the query
            logging.error(f"Failed to execute query: {e}")
            # Return None to indicate no records found
            return None
        
    def execute_read_request(self, start_time:int, stop_time:int) -> TableList:
        """
        Executes a read request to the InfluxDB for location data between the specified start and stop times.

        Parameters:
            start_time (int): The start time in seconds since epoch.
            stop_time (int): The stop time in seconds since epoch.

        Returns:    
            table_list (TableList): The resulting entries of the query from the InfluxDB as a TableList.
        """
        # Use the query_builder method to create a Flux query string with the provided start and stop times, execute the query using the read_data method and return the resulting table list
        return self.read_data(self.query_builder(start_time, stop_time))

class Main:
    """
    Main class to provide a SOAP service via an HTTPS server.
    """
    
    def __init__(self):
        """
        Initializes the Main class, reads the configuration, sets up logging, creates InfluxDB reader, creates SOAP service, creates server instance, and sets up SSL context.
        """
        # Initialize the configuration with the read_config method
        self.read_config()
        # Set up logging with the setup_logging method
        self.setup_logging()
        # Create an instance of the InfluxDBReader class with the create_influxdb_reader_instance method
        self.create_influxdb_reader_instance()
        # Create an instance of the PositionHistoryServiceApplication class with the create_soap_service_instance method
        self.create_soap_service_instance()
        # Create an instance of the AIOSpyne handler and set up the web application with CORS with the create_server_instance method
        self.create_server_instance()
        # Create an SSL context for secure communication with the create_ssl_context method
        self.create_ssl_context()
    
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
        logging.info(f"Welcome to the Position History Service (PHS) SOAP API Service Provider - the current time is {datetime.now()}")
        # Log completion of the logging setup
        logging.info("Logging setup complete")

    def create_influxdb_reader_instance(self):
        """
        Function to create an instance of the InfluxDBReader class.

        Parameters:
            self
        """
        # Create an instance of the InfluxDBReader class with the URL, token, organization, bucket, measurement, source, and CA certificate from the configuration file and save it in the class context
        self.influx_reader = InfluxDBReader(
            url=self.config.get('influxdb', 'url'),
            token=self.config.get('influxdb', 'token'),
            org=self.config.get('influxdb', 'org'),
            bucket=self.config.get('influxdb', 'bucket'),
            measurement=self.config.get('influxdb', 'measurement'),
            source=self.config.get('iss-api', 'source'),
            ca_cert=self.config.get('influxdb', 'ca_cert')
        )
        # Log the successful creation of the InfluxDBReader instance
        logging.info("InfluxDB Reader instance created")

    def create_soap_service_instance(self):
        """
        Function to create an instance of the PositionHistoryServiceApplication class which is a customized SpyneApplication.

        Parameters:
            self
        """
        # Create an instance of the PositionHistoryServiceApplication based on SpyneApplication with the services, target namespace, input and output protocols, and InfluxDB reader instance from the class context
        self.phs_application = PositionHistoryServiceApplication(
            services=[PositionHistoryService],
            tns=self.config.get('api-provider', 'namespace'),
            in_protocol=Soap12(validator='lxml'),
            out_protocol=Soap12(),
            influx_reader=self.influx_reader
        )
        # Log the successful creation of the PositionHistoryServiceApplication instance
        logging.info("PositionHistoryServiceApplication instance created")

    def create_server_instance(self):
        """
        Function to create an instance of the AIOSpyne handler and set up the web application with CORS.

        Parameters:
            self
        """
        # Create an instance of the AIOSpyne handler with the PositionHistoryServiceApplication instance and save it in the class context
        self.phs_handler = AIOSpyne(self.phs_application)
        # Create a web application instance 
        self.phs_web_app = web.Application()
        # Set up the routes for GET requests with the AIOSpyne handler
        get_route = self.phs_web_app.router.add_get('/{tail:.*}', self.phs_handler.get)
        # Set up the routes for POST requests with the AIOSpyne handler
        post_route = self.phs_web_app.router.add_post('/{tail:.*}', self.phs_handler.post)
        # Set up CORS (Cross-Origin Resource Sharing) for the web application
        cors = aiohttp_cors.setup(
            self.phs_web_app,
            defaults =
            {
                "https://ntgddns.asuscomm.com": aiohttp_cors.ResourceOptions(
                    allow_credentials=True,
                    expose_headers="*",
                    allow_headers="*",
                )
            }
        )
        # Add CORS support for the GET routes
        cors.add(get_route)
        # Add CORS support for the POST routes
        cors.add(post_route)
        # Log the successful creation of the server instance
        logging.info("Server instance created")

    def create_ssl_context(self):
        """
        Function to create an SSL context for secure communication.

        Parameters:
            self
        """
        # Create a default SSL context for client authentication
        self.ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        # Load the server certificate and key from the configuration file into the SSL context
        self.ssl_context.load_cert_chain(certfile=self.config.get("api-provider", "server_cert"), keyfile=self.config.get("api-provider", "server_key"))
        # Log the successful creation of the SSL context
        logging.info("SSL context created")

    def server_loop(self):
        """
        Function to start the server loop for the SOAP service web application.
        """
        # Log the start of the server loop
        logging.info("Starting server loop")
        # Run the web application with the specified port from the configuration file and the created SSL context 
        web.run_app(self.phs_web_app, port=self.config.getint('api-provider', 'port'), ssl_context=self.ssl_context)

if __name__ == "__main__":
    # Create an instance of the Main class
    main = Main()
    # Start the server loop to run the SOAP service web application
    main.server_loop()