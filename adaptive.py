import logging
from os import getenv
import xml.etree.ElementTree as ET

import requests


class Adaptive:
    def __init__(self, version):
        self.url = getenv("API_URL")
        self.login = getenv("API_USER")
        self.password = getenv("API_PWD")
        self.caller_name = getenv("CALLER_NAME")
        self.top_level = getenv("TOP_LEVEL")
        self.version = version

    def _format_xml_request(self, method, method_data=None):
        """Format the XML request to be sent to Adaptive.
        All requests follow the same basic format, with method specific data within.
        
        Example format:
        <?xml version='1.0' encoding='UTF-8'?>
        <call method="exportData" callerName="a string that identifies your client application">
            <credentials login="sample@company.com" password="my_pwd"/>              
            method specific data goes here
        </call>
        """
        # Set call element attributes
        call = ET.Element("call")
        call.set("method", method)
        call.set("callerName", self.caller_name)
        # Set credentials element attributes
        credentials = ET.SubElement(call, "credentials")
        credentials.set("login", self.login)
        credentials.set("password", self.password)
        # Set optional method-specific data
        if method_data is not None:
            for i in range(0, len(method_data)):
                call.insert(i + 1, method_data[i])
        # Convert to XML string
        request_string = ET.tostring(call, encoding="UTF-8", method="xml")
        return request_string

        def export_data(self):
            pass
