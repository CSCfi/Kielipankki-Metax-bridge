import json
from lxml import etree
import requests


class MSRecordParser:
    def __init__(self, record):
        """
        Create a Metashare record object.

        :param record: a Metashare record
        """
        xml_string = etree.tostring(record.xml)
        self.record_tree = etree.fromstring(xml_string)


    def _get_language_contents(self, xpath):
        """
        Retrieve the content from XML tree and XPath expression for different language versions.

        :param xpath: The XPath expression to select the desired trees.
        :return: A dictionary of content for different language versions.
            """
        result = {}

        languages = ["en", "fi", "und"]

        for lang in languages:
            query = self.record_tree.xpath(f"{xpath}[@lang='{lang}']/text()", namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"})
            if query:
                result[lang] = query[0].strip()

        return result

    def _get_text_xpath(self, xpath):
        """
        Retrieves text content of the first element from given XPath expression.

        :param xpath: The XPath expression to select the desired trees.
        :return: The text content of the selected element.

        """
        return self.record_tree.xpath(xpath, namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"})[0]

    def json_converter(self):
        identifier = self.get_identifier()
        description = self.get_language_contents(self.record_tree, "//info:description")
        title = self.get_language_contents(self.record_tree, '//info:resourceName')


        output = {
            "persistent_identifier": identifier[0],
            "title": title,
            "description": description
        }

        return json.dumps(output)
