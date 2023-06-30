import json
from lxml import etree
import requests


class MSRecordParser:
    def __init__(self, ms_record):
        """
        Create a Metashare record object.

        :param ms_record: a Metashare record
        """
        ms_xml_string = etree.tostring(ms_record.xml)
        self.ms_record_tree = etree.fromstring(ms_xml_string)

    def get_identifier(self):
        """
        Get the metadata identifier.
        """
        return self.ms_record_tree.xpath("//info:identificationInfo/info:identifier/text()", namespaces={'info': 'http://www.ilsp.gr/META-XMLSchema'})

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
    
    def get_organization_info(self, org_name):
        koodistot_api = "https://koodistot.suomi.fi/codelist-api/api/v1/coderegistries/fairdata/codeschemes/organization/codes"
        
        try:
            response = requests.get(url = koodistot_api, params = {"prefLabel": org_name})
            data = response.json()

            if data["results"]:
                result = {}
                result["code"] = data["results"][0]["codeValue"]
                result["in_scheme"] = data["results"][0]["codeScheme"]["uri"]
                result["pref_label"] = data["results"][0]["prefLabel"]
                return result
            else: #the API returns 200 even when no appropriate organization is returned
                raise ValueError("No results found")
        except ValueError:
            return ""


    def get_metadata_creators(self):
        metadata_creators = self.ms_record_tree.xpath("//info:metadataCreator", namespaces={'info': 'http://www.ilsp.gr/META-XMLSchema'})

        actors = []
        for metadata_creator in metadata_creators:
            first_name = self.get_value(metadata_creator, "info:givenName")
            surname = self.get_value(metadata_creator, "info:surname")
            org_name = self.get_value(metadata_creator, "info:affiliation/info:organizationName")
            if self.get_organization_info(org_name) == "":
                code = ""
                in_scheme = ""
                pref_label = self.get_language_contents(metadata_creator, "info:affiliation/info:organizationName")
            else:
                code = self.get_organization_info(org_name)["code"]
                in_scheme = self.get_organization_info(org_name)["in_scheme"]
                pref_label = self.get_organization_info(org_name)["pref_label"]

            actor = {
                "person": f"{first_name} {surname}",
                "organization": {
                    "code": code,
                    "in_scheme": in_scheme,
                    "pref_label": pref_label
                }
            }

            actors.append({
                "role": "creator",
                "actor": actor
            })

        return {"actors": actors}

    def json_converter(self):
        identifier = self.get_identifier()
        description = self.get_language_contents(self.ms_record_tree, "//info:description")
        metadata_creators = self.get_metadata_creators()
        title = self.get_language_contents(self.ms_record_tree, '//info:resourceName')


        output = {
            "persistent_identifier": identifier[0],
            "title": title,
            "description": description,
            "actors": metadata_creators["actors"]
        }

        return json.dumps(output)
