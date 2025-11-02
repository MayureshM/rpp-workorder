''' Libraries for interacting with recon services '''
import logging
import sys
import traceback
import requests
import boto3

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


class ReconServices:
    '''
    Class for interacting with recon services
    Used to manage any given request to recon services.
    Used to provide default base_url and headers
    '''

    _base_url = None
    _xs_header = None

    def __init__(self, base_url=None, xs_header=None):
        client = boto3.client('ssm')

        if base_url is None:
            self._base_url = client.get_parameter(
                Name='/ReconMVS/InspectionPlatform/BaseURL',
                WithDecryption=False
            )["Parameter"]["Value"]
        else:
            self._base_url = base_url

        if xs_header is None:
            self._xs_header = client.get_parameter(
                Name='/ReconMVS/InspectionPlatform/XSHeader',
                WithDecryption=True
            )["Parameter"]["Value"]
        else:
            self._xs_header = xs_header

    def get_service(self, service_url, query_params):
        ''' handles a GET request to recon services given the service url and params '''

        url = self._base_url + service_url
        headers = {"XS": self._xs_header}

        try:
            r = requests.get(url=url, headers=headers, params=query_params)  # nosec B113
            LOGGER.info("Calling %s. Status: %s, %s", str(url), str(r.status_code), str(r))
            if r.status_code != 200:
                return False, url, r.text, {
                    "status_code": str(r.status_code),
                    'headers': str(r.headers),
                    "body": {"recon_service_error": str(r.text)}
                }
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            LOGGER.error(e)
            LOGGER.error(lines)
            return False, url, str(e), {
                "status_code": 400,
                'headers': {'Content-Type': 'application/json'},
                "body": {"recon_service_error": str(e)}
            }

        data = r.json()

        LOGGER.info("Result from %s. Status: %s. Data: %s", str(url), str(r.status_code), str(data))

        return True, url, '', {
            "status_code": r.status_code,
            'headers': r.headers,
            "body": data
        }
