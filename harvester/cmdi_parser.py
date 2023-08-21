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
            query = self.xml.xpath(f"{xpath}[@lang='{lang}']/text()", namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"})
            if query:
                result[lang] = query[0].strip()

        return result

    def _get_text_xpath(self, xpath):
        """
        Retrieves text content of the first element from given XPath expression.

        :param xpath: The XPath expression to select the desired trees.
        :return: The text content of the selected element.

        """
        return self.xml.xpath(xpath, namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"})[0]

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

    def _check_metadatainfo_exists(self):
        """
        Only records with PIDs are relevant.
        """
        metadata = self.xml.xpath("//info:metadataInfo", namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"})
        if metadata:
            urn = self.xml.xpath("//info:identificationInfo/info:identifier", namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"})
            if not urn:
                return False
        if not metadata:
            return False
        return True
    
    def _get_list_of_licenses(self):
        """
        Retrieves all licenseInfo elements.
        """
        license_elements_list = self.xml.xpath("//info:distributionInfo/info:licenceInfo", namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"})
        if license_elements_list:
            return license_elements_list

    def _get_resourcetype_corpus(self):
        """
        Helper method to only retrieve "corpus" records.
        """
        resourcetype = self._get_text_xpath("//info:resourceType/text()")
        if resourcetype == "corpus":
            return True

    def _get_license_url_from_documentation(self):
        """
        Retrieves the license url for RES corpora.
        """
        doc_elements = self.xml.xpath("//info:resourceDocumentationInfo/info:documentation", namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"})

        for doc_element in doc_elements:
            doc_unstruct_element = doc_element.xpath("info:documentUnstructured/text()", namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"})
            doc_info_elements = doc_element.xpath("info:documentInfo", namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"})

            if doc_unstruct_element:
                doc_unstruct_text = doc_unstruct_element[0].lower()
                if "license:" in doc_unstruct_text or "license" in doc_unstruct_text:
                    license_urn = [word for word in doc_unstruct_text.split() if word.startswith("http://urn.fi")]
                    if license_urn:
                        return license_urn[0]
            
            elif doc_info_elements:
                for doc_info_elem in doc_info_elements:
                    title_element = doc_info_elem.xpath("info:title[@lang='en']/text()", namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"})
                    if title_element and "license" in title_element[0].lower():
                        license_urn = doc_info_elem.xpath("info:url/text()", namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"})
                        if license_urn:
                            return license_urn[0]

        return None

    def _get_license_information(self, license_element, mapped_licenses_dict):
        """
        Retrieves the license and its possible url (only applies to RES and "other" licenses) to a dictionary.
        """
        license_dict = {}

        license_text = license_element.xpath("info:licence/text()", namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"})[0]

        if license_text in mapped_licenses_dict:
            license_dict["url"] = mapped_licenses_dict[license_text]

            if license_text == "CLARIN_RES" or license_text == "other":
                custom_url = self._get_license_url_from_documentation()
                if custom_url:
                    license_dict["custom_url"] = custom_url
                
        return license_dict

    def _get_access_type(self):
        """
        Retrieves and maps access type to a dictionary.
        """
        access_type_dict = {}

        availability = self.xml.xpath("//info:distributionInfo/info:availability/text()", namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"})[0]
        if availability == "available-unrestrictedUse":
            access_type_dict["access_type"] = "http://uri.suomi.fi/codelist/fairdata/access_type/code/open"

        else:            
            access_type_dict["url"] = "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"
        
        return access_type_dict


    def json_converter(self):
        """
        Converts text and dictionaries to JSON.
        """

        output = {
            "persistent_identifier": self._get_identifier("//info:identificationInfo/info:identifier/text()"),
            "title": self._get_language_contents("//info:resourceName"),
            "description": self._get_language_contents("//info:description"),
            "modified": self._get_date("//info:metadataInfo/info:metadataLastDateUpdated/text()"),
            "issued": self._get_date("//info:metadataInfo/info:metadataCreationDate/text()"),
            "access_rights": self._map_access_rights()
        }

        return json.dumps(output)
