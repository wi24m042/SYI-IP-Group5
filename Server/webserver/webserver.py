import os
import ssl
import json
import logging
from aiohttp import web
from configparser import ConfigParser
from datetime import datetime
from zeep import Client
from zeep.transports import Transport
from zeep.helpers import serialize_object
import requests
from jsonschema import validate, ValidationError

class SoapClient:
    """
    A client for interacting with a SOAP web service using the Zeep library.
    """

    def __init__(self, wsdl_url:str):
        """
        Initializes the SOAP client with the provided WSDL URL.

        Parameters:
            wsdl_url (str): The URL of the WSDL file for the SOAP service.
        """
        # Save the WSDL URL in the class context
        self.wsdl_url = wsdl_url
        # Create a SOAP client with the create_soap_client method
        self.create_soap_client()

    def create_soap_client(self):
        """
        Function to creates a HTTPS capable SOAP client.

        Parameters:
            self
        """
        # Create a requests session for the SOAP client
        self.session = requests.Session()
        # Try to create the SOAP client using the provided WSDL URL and requests session
        try:
            # Create a SOAP client using the WSDL URL and the requests session and save it in the class context
            self.soap_client = Client(wsdl=self.wsdl_url, transport=Transport(session=self.session))
            # Log the successful creation of the SOAP client
            logging.info(f"SOAP client created with WSDL URL: {self.wsdl_url}")
        # Handle any exceptions that occur during the creation of the SOAP client
        except Exception as e:
            # Log the error message
            logging.error(f"Error creating SOAP client: {e}")
            # Raise the exception to indicate failure
            raise

    def get_location_history(self, start_time:int, stop_time:int) -> list | None:
        """
        Function to fetch location history from the SOAP service.

        Parameters:
            start_time (int): The start time for the location history query.
            stop_time (int): The end time for the location history query.

        Returns:
            response (list | None): A list of location history entries or None if an error occurs.
        """
        # Try to fetch the location history from the SOAP service
        try:
            # Log the start and stop times for the location history query
            logging.info(f"Fetching location history from {start_time} to {stop_time}")
            # Call the SOAP service with the SOAP client to get the location history
            response = self.soap_client.service.GetLocationHistory(start_time, stop_time)
            # Log the successful fetching of the location history with the response
            logging.info(f"Location history fetched successfully: {response}")
            # Return the response from the SOAP service
            return response
        # Handle any exceptions that occur during the fetching of the location history
        except Exception as e:
            # Log the error message
            logging.error(f"Error fetching location history: {e}")
            # Return None to indicate failure
            return None
        
    def get_closest_entry_by_timestamp(self, timestamp:int) -> list | None:
        """
        Function to fetch the closest entry by timestamp from the SOAP service.

        Parameters:
            timestamp (int): The timestamp to query for the closest entry.

        Returns:
            response (list | None): A list containing the closest entry or None if an error occurs.
        """
        # Try to fetch the closest entry by timestamp from the SOAP service
        try:
            # Log the timestamp for which the closest entry is being fetched
            logging.info(f"Fetching closest entry by timestamp: {timestamp}")
            # Call the SOAP service with the SOAP client to get the closest entry by timestamp
            response = self.soap_client.service.GetClosestEntryByTimestamp(timestamp=timestamp)
            # Log the successful fetching of the closest entry with the response
            logging.info(f"Closest entry fetched successfully: {response}")
            # Return the response from the SOAP service as a list
            return [response]
        # Handle any exceptions that occur during the fetching of the closest entry
        except Exception as e:
            # Log the error message
            logging.error(f"Error fetching closest entry by timestamp: {e}")
            # Return None to indicate failure
            return None

class ServerBackend:
    """
    A backend server for handling REST API requests and serving static files.
    """

    def __init__(self, soap_client:SoapClient, schema_filepath:str):
        """
        Initializes the server backend with a SOAP client and a JSON schema.

        Parameters:
            soap_client (SoapClient): An instance of the SoapClient to interact with the SOAP service.
            schema_filepath (str): The file path to the JSON schema for request validation.
        """
        # Save the SOAP client in the class context
        self.soap_client = soap_client
        # Load the JSON schema from the specified file path with the load_json_schema method and save it in the class context
        self.json_schema = self.load_json_schema(schema_filepath)
        # Save the base directory for the server backend
        self.base_dir = os.path.dirname(os.path.abspath(__file__))

    def load_json_schema(self, schema_filepath: str) -> dict:
        """
        Function to load the JSON schema from the specified file path.

        Parameters:
            schema_filepath (str): The file path to the JSON schema.

        Returns:
            dict: The loaded JSON schema as a dictionary.
        """
        # Open the schema file 
        with open(schema_filepath, "r") as file:
            # Load the file content as JSON schema and return it
            return json.load(file)

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
            logging.info("API request is valid according to the JSON schema.")
            # Return True if validation is successful
            return True
        # Handle validation errors
        except ValidationError as e:
            # Log the error message when validation fails
            logging.error(f"JSON schema validation failed: {e}")
            # Return False when validation fails
            return False
            
    async def handle_index(self, request:web.Request) -> web.FileResponse:
        """
        Function to handle requests for the index page.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.FileResponse): The response containing the index.html file.
        """
        # Return the index.html file as the response to the request
        return web.FileResponse("html/index.html")

    async def handle_favicon(self, request:web.Request) -> web.FileResponse:
        """
        Function to handle requests for the favicon.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.FileResponse): The response containing the PHS_favicon.png file.
        """
        # Return the PHS_favicon.png file as the response to the request
        return web.FileResponse("html/resources/PHS_favicon.png")

    async def handle_PHSicon(self, request:web.Request) -> web.FileResponse:
        """
        Function to handle requests for the PHS icon.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.FileResponse): The response containing the PHS_icon-small.png file.
        """
        # Return the PHS_icon-small.png file as the response to the request
        return web.FileResponse("html/resources/PHS_icon-small.png")

    async def handle_leaflet_js(self, request:web.Request) -> web.FileResponse:
        """
        Function to handle requests for the Leaflet JavaScript file.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.FileResponse): The response containing the leaflet.icon-material.js file.
        """
        # Return the leaflet.icon-material.js file as the response to the request
        return web.FileResponse("html/resources/leaflet.icon-material.js")

    async def handle_leaflet_css(self, request:web.Request) -> web.FileResponse:
        """
        Function to handle requests for the Leaflet CSS file.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.FileResponse): The response containing the leaflet.icon-material.css file.
        """
        # Return the leaflet.icon-material.css file as the response to the request
        return web.FileResponse("html/resources/leaflet.icon-material.css")

    async def handle_script_js(self, request:web.Request) -> web.FileResponse:
        """
        Function to handle requests for the script JavaScript file.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.FileResponse): The response containing the script.js file.
        """
        # Return the script.js file as the response to the request
        return web.FileResponse("html/frontend/script.js")

    async def handle_styles_css(self, request:web.Request) -> web.FileResponse:
        """
        Function to handle requests for the styles CSS file.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.FileResponse): The response containing the styles.css file.
        """
        # Return the styles.css file as the response to the request
        return web.FileResponse("html/frontend/styles.css")

    async def handle_get_location_history(self, request:web.Request) -> web.Response:
        """
        Function to handle requests for getting location history.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.Response): The JSON list response containing the location history.
        """
        # Try to handle the request for getting location history
        try:
            # Wait for the requests with JSON data
            rest_data = await request.json()
            # Log the received request data
            logging.info(f"Received request data: {rest_data}")
            # Validate the JSON data against the schema
            if self.validate_json(rest_data):
                # When the JSON data is valid, extract the start time for the location history query
                start_time = rest_data.get("PositionHistoryService", {}).get("GetLocationHistory", {}).get("StartTime")
                # When the JSON data is valid, extract the stop time for the location history query
                stop_time = rest_data.get("PositionHistoryService", {}).get("GetLocationHistory", {}).get("StopTime")
                # Use the get_location_history method of the SOAP client instance to get the location history with the start and stop times from the SOAP service
                response = self.soap_client.get_location_history(start_time, stop_time)
                # If the response is None
                if response is None:
                    # Return an empty JSON list with a 400 Bad Request status
                    return web.json_response([], status=400)
                # Return the response as a JSON list after serializing the objects
                return web.json_response(response, dumps=lambda obj: json.dumps(serialize_object(obj), indent=2))
            # If the JSON data is not valid according to the schema
            else:
                # Log an error message indicating that the JSON schema validation failed
                logging.error("Invalid JSON schema")
                # Return an empty JSON list with a 400 Bad Request status
                return web.json_response([], status=400)
        # Handle any exceptions that occur during the request handling
        except Exception as e:
            # Log the error message indicating an internal error
            logging.error(f"Internal error: {e}")
            # Wait for the requests with POST data
            data = await request.post()
            # Log the received POST request data
            logging.error(f"Received post request data: {data}")
            # Return a JSON response with the received data
            return web.json_response({"received": data})
    
    async def handle_get_closest_entry_by_timestamp(self, request:web.Request) -> web.Response:
        """
        Function to handle requests for getting the closest entry by timestamp.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.Response): The JSON list response containing the closest entry by timestamp.
        """

        try:
            # Wait for the requests with JSON data
            rest_data = await request.json()
            # Log the received request data
            logging.info(f"Received request data: {rest_data}")
            # Validate the JSON data against the schema
            if self.validate_json(rest_data):
                # When the JSON data is valid, extract the timestamp for the closest entry query
                timestamp = rest_data.get("PositionHistoryService", {}).get("GetClosestEntryByTimestamp", {}).get("Timestamp")
                # When the JSON data is valid, use the get_closest_entry_by_timestamp method of the SOAP client instance to get the closest entry by timestamp from the SOAP service
                response = self.soap_client.get_closest_entry_by_timestamp(timestamp)
                logging.info(f"Response from SOAP service: {response}")
                logging.info(f"Response type: {type(response)}")
                # If the response is None
                if response == [None]:
                    # Return an empty JSON list with a 400 Bad Request status
                    return web.json_response([], status=400)
                # Return the response as a JSON list after serializing the objects
                return web.json_response(response, dumps=lambda obj: json.dumps(serialize_object(obj), indent=2))
            else:
                # Log an error message indicating that the JSON schema validation failed
                logging.error("Invalid JSON schema")
                # Return an empty JSON list with a 400 Bad Request status
                return web.json_response([], status=400)
        # Handle any exceptions that occur during the request handling
        except Exception as e:
            # Log the error message indicating an internal error
            logging.error(f"Internal error: {e}")
            # Wait for the requests with POST data
            data = await request.post()
            # Log the received POST request data
            logging.error(f"Received post request data: {data}")
            # Return a JSON response with the received data
            return web.json_response({"received": data})

    async def handle_404(self, request:web.Request) -> web.Response:
        """
        Function to handle 404 Not Found errors.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.Response): The response indicating a 404 Not Found error.
        """
        # Return a 404 Not Found response with the requested path
        return web.Response(status=404, text=f"404 not found: {request.path}")

class Main:
    """
    Main class to initialize and run the web server.
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
        # Create the SOAP client instance with the create_soap_client method
        self.create_soap_client()
        # Create the server backend with the create_backend method
        self.create_backend()
        # Create the web application instance with the create_server_instance method
        self.create_server_instance()
        # Create the SSL context with the create_ssl_context method
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
        logging.info(f"Welcome to the Position History Service (PHS) web server - the current time is {datetime.now()}")
        # Log completion of the logging setup
        logging.info("Logging setup complete")

    def create_soap_client(self):
        """
        Function to create a SOAP client using the WSDL URL from the configuration.

        Parameters:
            self
        """
        # Create a SOAP client instance with the WSDL URL from the configuration
        self.soap_client = SoapClient(self.config.get("soap", "wsdl_url"))

    def create_backend(self):
        """
        Function to create the server backend with the SOAP client and JSON schema.

        Parameters:
            self
        """
        # Create a ServerBackend class instance with the SOAP client and the JSON schema file path from the configuration
        self.backend = ServerBackend(self.soap_client, self.config.get("rest-api", "rest_api_schema"))
        # Log the successful creation of the server backend
        logging.info("Server backend created")

    def create_server_instance(self):
        """
        Function to create the web application instance and set up the routes.

        Parameters:
            self
        """
        # Create a web application instance using aiohttp
        self.web_app = web.Application()
        # Set up the route to handle get requests for the index page with the handle_index method from the backend class instance
        self.web_app.router.add_get("/", self.backend.handle_index)
        # Set up the route to handle get requests for the index page with the handle_index method from the backend class instance
        self.web_app.router.add_get("/index.html", self.backend.handle_index)
        # Set up the route to handle get requests for the favicon with the handle_favicon method from the backend class instance
        self.web_app.router.add_get("/resources/PHS_favicon.png", self.backend.handle_favicon)
        # Set up the route to handle get requests for the PHS icon with the handle_PHSicon method from the backend class instance
        self.web_app.router.add_get("/resources/PHS_icon-small.png", self.backend.handle_PHSicon)
        # Set up the route to handle get requests for the script.js file with the handle_script_js method from the backend class instance
        self.web_app.router.add_get("/frontend/script.js", self.backend.handle_script_js)
        # Set up the route to handle get requests for the styles.css file with the handle_styles_css method from the backend class instance
        self.web_app.router.add_get("/frontend/styles.css", self.backend.handle_styles_css)
        # Set up the route to handle get requests for the leaflet.icon-material.js file with the handle_leaflet_js method from the backend class instance
        self.web_app.router.add_get("/resources/leaflet.icon-material.js", self.backend.handle_leaflet_js)
        # Set up the route to handle get requests for the leaflet.icon-material.css file with the handle_leaflet_css method from the backend class instance
        self.web_app.router.add_get("/resources/leaflet.icon-material.css", self.backend.handle_leaflet_css)
        # Set up the route to handle post requests for the get_location_history API with the handle_get_location_history method from the backend class instance
        self.web_app.router.add_post("/api/get_location_history", self.backend.handle_get_location_history)
        # Set up the route to handle post requests for the get_closest_entry_by_timestamp API with the handle_get_closest_entry_by_timestamp method from the backend class instance
        self.web_app.router.add_post("/api/get_closest_entry_by_timestamp", self.backend.handle_get_closest_entry_by_timestamp)
        # Set up a catch-all route to handle 404 Not Found errors with the handle_404 method from the backend class instance
        self.web_app.router.add_route("*", "/{tail:.*}", self.backend.handle_404)
        # Log the successful creation of the web application instance
        logging.info("Web application instance created")

    def create_ssl_context(self):
        """
        Function to create an SSL context for secure connections.

        Parameters:
            self
        """
        # Create a default SSL context for client authentication
        self.ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        # Load the server certificate and key from the configuration file into the SSL context
        self.ssl_context.load_cert_chain(certfile=self.config.get("webserver", "server_cert"), keyfile=self.config.get("webserver", "server_key"))
        # Log the successful creation of the SSL context
        logging.info("SSL context created")

    def server_loop(self):
        """
        Function to start the web server loop.

        Parameters:
            self
        """
        # Log the start of the server loop
        logging.info("Starting server loop")
        # Run the web application with the specified port from the configuration file and the created SSL context 
        web.run_app(self.web_app, port=self.config.getint("webserver", "port"), ssl_context=self.ssl_context)

if __name__ == "__main__":
    # Create an instance of the Main class
    main = Main()
    # Start the server loop to run the SOAP service web application
    main.server_loop()