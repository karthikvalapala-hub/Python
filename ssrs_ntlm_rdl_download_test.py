import os
import getpass
import requests
import xml.etree.ElementTree as ET
from requests_ntlm import HttpNtlmAuth
from urllib.parse import quote

SSRS_SERVER_URL = 'https://your-server/ReportServer'
REPORT_PATH = '/Products/ProductSales'
DOWNLOAD_FOLDER = os.path.join(os.getcwd(), 'SSRS_RDL_Downloads')


def main():
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
    username = input('Enter DOMAIN\\username: ')
    password = getpass.getpass('Enter Windows password: ')
    with requests.Session() as session:
        session.auth = HttpNtlmAuth(username, password)
        session.verify = True
        try:
            response = session.get(SSRS_SERVER_URL, timeout=60)
            response.raise_for_status()
            print('SSRS HTTP access successful. Status:', response.status_code)
        except requests.RequestException as exc:
            print('Connection failed:', exc)
            return
        # This is a diagnostic only. GetResourceContents is not a supported
        # generic RDL download command for catalog reports.
        url = (SSRS_SERVER_URL.rstrip('/') + '?/' +
               quote(REPORT_PATH.lstrip('/'), safe='/') +
               '&rs:Command=GetResourceContents')
        try:
            response = session.get(url, timeout=120)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            if root.tag.split('}')[-1] != 'Report':
                raise ValueError('Response is not RDL XML (root: %s)' % root.tag)
            report_name = REPORT_PATH.strip('/').split('/')[-1]
            destination = os.path.join(DOWNLOAD_FOLDER, report_name + '.rdl')
            with open(destination, 'wb') as file:
                file.write(response.content)
            print('Saved valid RDL:', destination)
        except (requests.RequestException, ET.ParseError, ValueError) as exc:
            print('RDL download unavailable via this URL access method:', exc)
            print('Ask SSRS administrators for an approved RDL export method.')


if __name__ == '__main__':
    main()
