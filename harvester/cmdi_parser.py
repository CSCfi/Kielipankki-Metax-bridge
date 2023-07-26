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

    def get_identifier(self):
        """
        Get the metadata identifier.
        """
        return self.record_tree.xpath("//info:identificationInfo/info:identifier/text()", namespaces={'info': 'http://www.ilsp.gr/META-XMLSchema'})

    def get_language_contents(self, element, xpath, output_field=None):
        """
        Retrieve the content from the specified element and XPath expression for different language versions.

        :param element: The XML element to extract content from.
        :param xpath: The XPath expression to select the desired elements.
        :param output_field: Optional output field name for the result.
        :return: A dictionary of content for different language versions if output_field is provided,
                    otherwise, a dictionary of content directly.
                    Returns None if no content is found.
            """
        result = {}

        languages = ["en", "fi", "und"]

        for lang in languages:
            query = element.xpath(f'{xpath}[@lang="{lang}"]/text()', namespaces={'info': 'http://www.ilsp.gr/META-XMLSchema'})
            if query:
                result[lang] = query[0].strip()

        if result:
            if output_field:
                return {output_field: result}
            else:
                return result
        else:
            return None

    def get_value(self, element, xpath, output_field=None):
        query = element.xpath(xpath, namespaces={'info': 'http://www.ilsp.gr/META-XMLSchema'})

        result = [node.text.strip() if node.text else "" for node in query]
        result = [content for content in result if content]

        return result[0] if result else None
    

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
