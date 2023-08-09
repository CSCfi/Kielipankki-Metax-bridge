import json
from lxml import etree
from urllib.parse import urlparse 
from datetime import datetime


class MSRecordParser:
    def __init__(self, xml):
        """
        Create a Metashare record object.

        :param xml: an lxml object, representing a CMDI record
        """
        self.xml = xml

    def _get_language_contents(self, xpath):
        """
        Retrieve the content from XML tree and XPath expression for different language versions.

        :param xpath: The XPath expression to select the desired trees.
        :return: A dictionary of content for different language versions.
            """
        result = {}

        languages = ["en", "fi", "und"]

        for lang in languages:
            query = self.xml.xpath(f"{xpath}[@xml:lang='{lang}']/text()", namespaces={"cmd": "http://www.clarin.eu/cmd/"})
            if query:
                result[lang] = query[0].strip()

        return result

    def _get_text_xpath(self, xpath):
        """
        Retrieves text content of the first element from given XPath expression.

        :param xpath: The XPath expression to select the desired trees.
        :return: The text content of the selected element.

        """
        return self.xml.xpath(xpath, namespaces={"cmd": "http://www.clarin.eu/cmd/"})[0]

    def _get_identifier(self, xpath):
        """
        Retrieves the urn of the given XPath's url.
        """
        identifier_url = self._get_text_xpath(xpath)
        netloc, path = urlparse(identifier_url).netloc, urlparse(identifier_url).path
        return netloc + path
    
    def _get_date(self, xpath):
        """
        Retrieves the date of the given XPath and returns it  appropriate date-time format.
       
        """
        date_str = self._get_text_xpath(xpath)
        if date_str:
            datetime_obj = datetime.strptime(date_str, "%Y-%m-%d")
            formatted_date_str = datetime_obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            return formatted_date_str
        else:
            raise ValueError("No date found")

    def json_converter(self):
        """
        Converts text and dictionaries to JSON.
        """

        output = {
            "persistent_identifier": self._get_identifier("//cmd:identificationInfo/cmd:identifier/text()"),
            "title": self._get_language_contents("//cmd:resourceName"),
            "description": self._get_language_contents("//cmd:description"),
            "modified": self._get_date("//cmd:metadataInfo/cmd:metadataLastDateUpdated/text()"),
            "issued": self._get_date("//cmd:metadataInfo/cmd:metadataCreationDate/text()")
        }

        return json.dumps(output)
