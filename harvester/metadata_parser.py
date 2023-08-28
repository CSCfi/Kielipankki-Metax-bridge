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

    def get_identifier(self, xpath):
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

    def _check_pid_exists(self):
        """
        Only records with PIDs are relevant.
        """
        urn = self.xml.xpath("//info:identificationInfo/info:identifier", namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"})

        return bool(urn)
    
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
        Retrieves the license url.
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
        Retrieves the license and its possible url to a dictionary.
        """
        license_dict = {}

        license_text = license_element.xpath("info:licence/text()", namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"})[0]

        if license_text in mapped_licenses_dict:
            license_dict["url"] = mapped_licenses_dict[license_text]
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
            access_type_dict["url"] = "http://uri.suomi.fi/codelist/fairdata/access_type/code/open"

        else:            
            access_type_dict["url"] = "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"
        
        return access_type_dict

    def _map_access_rights(self):
        """
        Retrieves and maps all license and access type information to a dictionary.
        """
        license_package = {}
        license_mappings = {
            "CLARIN_PUB": "http://uri.suomi.fi/codelist/fairdata/license/code/ClarinPUB-1.0",
            "CLARIN_ACA": "http://uri.suomi.fi/codelist/fairdata/license/code/ClarinACA-1.0",
            "CLARIN_ACA-NC": "http://uri.suomi.fi/codelist/fairdata/license/code/ClarinACA+NC-1.0",
            "CLARIN_RES": "http://uri.suomi.fi/codelist/fairdata/license/code/ClarinRES-1.0",
            "other": "http://uri.suomi.fi/codelist/fairdata/license/code/other",
            "underNegotiation": "http://uri.suomi.fi/codelist/fairdata/license/code/undernegotiation",
            "proprietary": "http://uri.suomi.fi/codelist/fairdata/license/code/proprietary",
            "CC-BY": "http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-1.0",
            "CC-BY-ND": "http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-ND4.0",
            "CC-BY-NC": "http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-NC2.0",
            "CC-BY-SA": "http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-SA4.0",
            "CC-BY-NC-ND": "http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-NC-ND4.0",
            "CC-BY-NC-SA": "http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-NC-SA4.0",
            "CC-ZERO": "http://uri.suomi.fi/codelist/fairdata/license/code/CC-ZERO"
        }

        license_elements_list = self._get_list_of_licenses()
        license_list = []

        if license_elements_list:
            for license_element in license_elements_list:
                license = self._get_license_information(license_element, license_mappings)
                license_list.append(license)   
        else:
            license = {"url": license_mappings["other"]}
            license_list.append(license)

        access_type = self._get_access_type()

        license_package["license"] = license_list
        license_package["access_type"] = access_type

        return license_package


    def json_converter(self):
        """
        Converts text and dictionaries to JSON.
        """

        if self._check_pid_exists(): #We may prefer to do this elsewhere later on
            if self._get_resourcetype_corpus(): #We may prefer to do this elsewhere later on

                output = {
                    #data_catalog, language and field_of_science is dummy data until they are implemented later on
                    "data_catalog": "urn:nbn:fi:att:data-catalog-kielipankki",
                    "language": [
                        {
                            "url": "http://lexvo.org/id/iso639-3/fin"
                        }
                    ],
                    "field_of_science": [
                        {
                            "url": "http://www.yso.fi/onto/okm-tieteenala/ta112",
                        }
                    ],
                    "persistent_identifier": self.get_identifier("//info:identificationInfo/info:identifier/text()"),
                    "title": self._get_language_contents("//info:resourceName"),
                    "description": self._get_language_contents("//info:description"),
                    "modified": self._get_date("//info:metadataInfo/info:metadataLastDateUpdated/text()"),
                    "issued": self._get_date("//info:metadataInfo/info:metadataCreationDate/text()"),
                    "access_rights": self._map_access_rights(),
                    
                }

                return json.dumps(output)
