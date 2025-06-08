import os
import ssl
import json
import logging
from aiohttp import web
from configparser import ConfigParser
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
        self.wsdl_url = wsdl_url
        self.create_soap_client()

    def create_soap_client(self):
        """
        Function to creates a HTTPS capable SOAP client.

        Parameters:
            self
        """
        self.session = requests.Session()
        try:
            self.soap_client = Client(wsdl=self.wsdl_url, transport=Transport(session=self.session))
            logging.info(f"SOAP client created with WSDL URL: {self.wsdl_url}")
        except Exception as e:
            logging.error(f"Error creating SOAP client: {e}")
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
        try:
            logging.info(f"Fetching location history from {start_time} to {stop_time}")
            response = self.soap_client.service.GetLocationHistory(start_time, stop_time)
            logging.info(f"Location history fetched successfully: {response}")
            return response
        except Exception as e:
            logging.error(f"Error fetching location history: {e}")
            return None
        
    def get_closest_entry_by_timestamp(self, timestamp:int) -> list | None:
        """
        Function to fetch the closest entry by timestamp from the SOAP service.

        Parameters:
            timestamp (int): The timestamp to query for the closest entry.

        Returns:
            response (list | None): A list containing the closest entry or None if an error occurs.
        """
        try:
            logging.info(f"Fetching closest entry by timestamp: {timestamp}")
            response = self.soap_client.service.GetClosestEntryByTimestamp(timestamp=timestamp)
            logging.info(f"Closest entry fetched successfully: {response}")
            return [response]
        except Exception as e:
            logging.error(f"Error fetching closest entry by timestamp: {e}")
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
        self.soap_client = soap_client
        self.json_schema = self.load_json_schema(schema_filepath)
        self.base_dir = os.path.dirname(os.path.abspath(__file__))

    def load_json_schema(self, schema_filepath: str) -> dict:
        """
        Function to load the JSON schema from the specified file path.

        Parameters:
            schema_filepath (str): The file path to the JSON schema.

        Returns:
            dict: The loaded JSON schema as a dictionary.
        """
        with open(schema_filepath, "r") as file:
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
        try:
            validate(instance=json_content, schema=self.json_schema)
            logging.info("API request is valid according to the JSON schema.")
            return True
        except ValidationError as e:
            logging.error(f"JSON schema validation failed: {e}")
            return False
            
    async def handle_index(self, request:web.Request) -> web.FileResponse:
        """
        Function to handle requests for the index page.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.FileResponse): The response containing the index.html file.
        """
        return web.FileResponse("html/index.html")

    async def handle_favicon(self, request:web.Request) -> web.FileResponse:
        """
        Function to handle requests for the favicon.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.FileResponse): The response containing the PHS_favicon.png file.
        """
        return web.FileResponse("html/resources/PHS_favicon.png")

    async def handle_PHSicon(self, request:web.Request) -> web.FileResponse:
        """
        Function to handle requests for the PHS icon.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.FileResponse): The response containing the PHS_icon-small.png file.
        """
        return web.FileResponse("html/resources/PHS_icon-small.png")

    async def handle_leaflet_js(self, request:web.Request) -> web.FileResponse:
        """
        Function to handle requests for the Leaflet JavaScript file.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.FileResponse): The response containing the leaflet.icon-material.js file.
        """
        return web.FileResponse("html/resources/leaflet.icon-material.js")

    async def handle_leaflet_css(self, request:web.Request) -> web.FileResponse:
        """
        Function to handle requests for the Leaflet CSS file.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.FileResponse): The response containing the leaflet.icon-material.css file.
        """
        return web.FileResponse("html/resources/leaflet.icon-material.css")

    async def handle_script_js(self, request:web.Request) -> web.FileResponse:
        """
        Function to handle requests for the script JavaScript file.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.FileResponse): The response containing the script.js file.
        """
        return web.FileResponse("html/frontend/script.js")

    async def handle_styles_css(self, request:web.Request) -> web.FileResponse:
        """
        Function to handle requests for the styles CSS file.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.FileResponse): The response containing the styles.css file.
        """
        return web.FileResponse("html/frontend/styles.css")

    async def handle_get_location_history(self, request:web.Request) -> web.Response:
        """
        Function to handle requests for getting location history.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.Response): The JSON list response containing the location history.
        """
        try:
            rest_data = await request.json()
            logging.info(f"Received request data: {rest_data}")
            if self.validate_json(rest_data):
                start_time = rest_data.get("PositionHistoryService", {}).get("GetLocationHistory", {}).get("StartTime")
                stop_time = rest_data.get("PositionHistoryService", {}).get("GetLocationHistory", {}).get("StopTime")
                response = self.soap_client.get_location_history(start_time, stop_time)
                if response is None:
                    response = []
                return web.json_response(response, dumps=lambda obj: json.dumps(serialize_object(obj), indent=2))
            else:
                logging.error("Invalid JSON schema")
                return web.json_response([], status=400)
        except Exception as e:
            logging.error(f"Internal error: {e}")
            data = await request.post()
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
            rest_data = await request.json()
            logging.info(f"Received request data: {rest_data}")
            if self.validate_json(rest_data):
                timestamp = rest_data.get("PositionHistoryService", {}).get("GetClosestEntryByTimestamp", {}).get("Timestamp")
                response = self.soap_client.get_closest_entry_by_timestamp(timestamp)
                return web.json_response(response, dumps=lambda obj: json.dumps(serialize_object(obj), indent=2))
            else:
                logging.error("Invalid JSON schema")
                return web.json_response([], status=400)
        except Exception as e:
            logging.error(f"Internal error: {e}")
            data = await request.post()
        return web.json_response({"received": data})

    async def handle_404(self, request:web.Request) -> web.Response:
        """
        Function to handle 404 Not Found errors.

        Parameters:
            request (web.Request): The incoming web request.

        Returns:
            response (web.Response): The response indicating a 404 Not Found error.
        """
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
        self.read_config()
        self.setup_logging()
        self.create_soap_client()
        self.create_backend()
        self.create_server_instance()
        self.create_ssl_context()
    
    def read_config(self):
        """
        Function to read the configuration from the config.ini file.

        Parameters:
            self
        """
        self.config = ConfigParser()
        self.config.read("config.ini")
    
    def setup_logging(self):
        """
        Function to set up logging configuration.

        Parameters:
            self
        """
        logging.basicConfig(
            filename=self.config.get("logging", "log_file"),
            level=logging.INFO,
            format="%(asctime)s %(levelname)s: %(message)s"
        )
        logging.info("Welcome to the Position History Service (PHS) web server - the current time is %s",)
        logging.info("Logging setup complete")

    def create_soap_client(self):
        """
        Function to create a SOAP client using the WSDL URL from the configuration.

        Parameters:
            self
        """
        self.soap_client = SoapClient(self.config.get("soap", "wsdl_url"))

    def create_backend(self):
        """
        Function to create the server backend with the SOAP client and JSON schema.

        Parameters:
            self
        """
        self.backend = ServerBackend(self.soap_client, self.config.get("rest-api", "rest_api_schema"))
        logging.info("Server backend created")

    def create_server_instance(self):
        """
        Function to create the web application instance and set up the routes.

        Parameters:
            self
        """
        self.web_app = web.Application()
        self.web_app.router.add_get("/", self.backend.handle_index)
        self.web_app.router.add_get("/index.html", self.backend.handle_index)
        self.web_app.router.add_get("/resources/PHS_favicon.png", self.backend.handle_favicon)
        self.web_app.router.add_get("/resources/PHS_icon-small.png", self.backend.handle_PHSicon)
        self.web_app.router.add_get("/frontend/script.js", self.backend.handle_script_js)
        self.web_app.router.add_get("/frontend/styles.css", self.backend.handle_styles_css)
        self.web_app.router.add_get("/resources/leaflet.icon-material.js", self.backend.handle_leaflet_js)
        self.web_app.router.add_get("/resources/leaflet.icon-material.css", self.backend.handle_leaflet_css)
        self.web_app.router.add_post("/api/get_location_history", self.backend.handle_get_location_history)
        self.web_app.router.add_post("/api/get_closest_entry_by_timestamp", self.backend.handle_get_closest_entry_by_timestamp)
        self.web_app.router.add_route("*", "/{tail:.*}", self.backend.handle_404)  # catch-all for 404
        logging.info("Web application instance created")

    def create_ssl_context(self):
        """
        Function to create an SSL context for secure connections.

        Parameters:
            self
        """
        self.ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        self.ssl_context.load_cert_chain(certfile=self.config.get("webserver", "server_cert"), keyfile=self.config.get("webserver", "server_key"))
        logging.info("SSL context created")

    def server_loop(self):
        """
        Function to start the web server loop.

        Parameters:
            self
        """
        logging.info("Starting server loop")
        web.run_app(self.web_app, port=self.config.getint("webserver", "port"), ssl_context=self.ssl_context)

if __name__ == "__main__":
    main = Main()
    main.server_loop()