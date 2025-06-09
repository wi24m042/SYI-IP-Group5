import click
import requests
from zeep import Client
from zeep.transports import Transport

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
        # Save the WSDL URL in class context
        self.wsdl_url = wsdl_url
        # Call the function to create the SOAP client
        self.create_soap_client()

    def create_soap_client(self):
        """
        Function to creates a HTTPS capable SOAP client.

        Parameters:
            self
        """
        # Create a requests session
        self.session = requests.Session()
        # Try to create a SOAP client with the provided WSDL URL and requests session
        try:
            # Create and save a SOAP client with the provided WSDL URL and requests session in the class context
            self.soap_client = Client(wsdl=self.wsdl_url, transport=Transport(session=self.session))
        # Handle any exceptions that occur during client creation
        except Exception as e:
            # Print the error message
            print(f"Error creating SOAP client: {e}")
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
        # Try to fetch location history from the SOAP service
        try:
            # Call the SOAP service to get location history between start_time and stop_time which are Unix timestamps
            response = self.soap_client.service.GetLocationHistory(start_time, stop_time)
            # Return the response list
            return response
        # Handle any exceptions that occur during the SOAP call
        except Exception as e:
            # Print the error message
            print(f"Error fetching location history: {e}")
            # Return None if an error occurs
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
            # Call the SOAP service to get the closest entry by timestamp which is a Unix timestamp
            response = self.soap_client.service.GetClosestEntryByTimestamp(timestamp=timestamp)
            # Return the response containing the closest entry
            return response
        # Handle any exceptions that occur during the SOAP call
        except Exception as e:
            # Print the error message
            print(f"Error fetching closest entry by timestamp: {e}")
            # Return None if an error occurs
            return None

# Create an instance of the SoapClient with the WSDL URL
soap_client = SoapClient("https://ntgddns.asuscomm.com:8000/?wsdl")


@click.group()
def main():
    """
    A simple command line interface for interacting with the SOAP service.
    """
    pass

@main.command()
@click.argument('start_time', type=int)
@click.argument('stop_time', type=int)
def location_history(start_time, stop_time):
    """
    Get location history between Unix timestamps.
    """
    # Fetch and print the location history
    print(soap_client.get_location_history(start_time, stop_time))

@main.command()
@click.argument('timestamp', type=int)
def closest_entry_by_timestamp(timestamp):
    """
    Get the closest entry by Unix timestamp.
    """
    # Fetch and print the closest entry by timestamp
    print(soap_client.get_closest_entry_by_timestamp(timestamp))

if __name__ == "__main__":
    main()