import logging
from os import getenv
import xml.etree.ElementTree as ET

import requests


class Adaptive:
    def __init__(self):
        self.url = getenv("API_URL")
        self.login = getenv("API_USER")
        self.password = getenv("API_PWD")
        self.caller_name = getenv("CALLER_NAME")
        self.version = getenv("VERSION")

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
        """Call Adaptive API exportData method"""
        # Build XML elements
        version = ET.Element("version")
        version.set("name", self.version)
        format_elmt = ET.Element("format")
        format_elmt.set("useInternalCodes", "true")
        format_elmt.set("includeUnmappedItems", "false")
        rules = ET.Element("rules")
        rules.set("timeRollups", "true")
        # Format the request and send to Adaptive
        method_data = [version, format_elmt, rules]
        xml = self._format_xml_request("exportData", method_data=method_data)
        logging.debug("Sent POST request exportData to Adaptive API")
        response = requests.post(self.url, data=xml)
        if response.status_code == 200:
            logging.debug(f"exportData request successful")
        else:
            raise Exception(f"exportData request not successful")
        return response.text

    def export_configurable_model_data(self, level_name):
        """Call Adaptive API exportConfigurableModelData method"""
        # Build XML elements
        version = ET.Element("version")
        version.set("name", self.version)
        job = ET.Element("job")
        job.set("jobNumber", "0")
        job.set("pageNumber", "1")
        job.set("pageSize", "200")
        modeled_sheet = ET.Element("modeled-sheet")
        modeled_sheet.set("name", "Personnel")
        modeled_sheet.set("isGlobal", "false")
        modeled_sheet.set("includeAllColumns", "true")
        filters = ET.Element("filters")
        time_span = ET.SubElement(filters, "timeSpan")
        time_span.set("start", getenv("PERSONNEL_START"))
        time_span.set("end", getenv("PERSONNEL_END"))
        levels = ET.SubElement(filters, "levels")
        level = ET.SubElement(levels, "level")
        level.set("name", level_name)
        # Format the request and send to Adaptive
        method_data = [version, job, modeled_sheet, filters]
        xml = self._format_xml_request(
            "exportConfigurableModelData", method_data=method_data
        )
        logging.debug("Sent POST request exportConfigurableModelData to Adaptive API")
        response = requests.post(self.url, data=xml)
        if response.status_code == 200:
            logging.debug(f"exportConfigurableModelData request successful")
        else:
            raise Exception(f"exportConfigurableModelData request not successful")
        return response.text
