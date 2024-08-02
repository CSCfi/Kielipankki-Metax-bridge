from datetime import datetime
from urllib.parse import urlparse

from lxml import etree
import iso639

from harvester.actor import Actor
from harvester import language_validator


class MSRecordParser:
    def __init__(self, xml):
        """
        Create a Metashare record object.

        :param xml: an lxml object, representing a CMDI record
        """
        self.xml = xml
        self.namespaces = {
            "cmd": "http://www.clarin.eu/cmd/",
            "oai": "http://www.openarchives.org/OAI/2.0/",
        }

    def _get_element_text_in_preferred_language(self, xpath):
        """
        Retrieve the content from XML tree and XPath expression for different language versions.

        :param xpath: The XPath expression to select the desired trees.
        :return: A dictionary of content for different language versions.
        """
        result = {}

        languages = ["en", "fi", "und"]

        for lang in languages:
            query = self.xml.xpath(
                f"{xpath}[@xml:lang='{lang}']/text()", namespaces=self.namespaces
            )
            if query:
                result[lang] = query[0].strip()

        return result

    def _get_text_xpath(self, xpath):
        """
        Retrieves text content of the first element from given XPath expression.

        :param xpath: The XPath expression to select the desired trees.
        :return: The text content of the selected element.

        """
        return self.xml.xpath(xpath, namespaces=self.namespaces)[0]

    def _get_identifier(self, xpath):
        """
        Retrieves the urn of the given XPath's url.
        """
        identifier_urn = self._get_text_xpath(xpath).strip()
        return f"urn.fi/{identifier_urn}"

    def _get_datetime(self, xpath):
        """
        Retrieve the datetime from given XPath as a string (YYYY-mm-ddTHH:MM:SSZ).

        Handles fields that are either dates (YYYY-mm-dd) or already datetimes
        (YYYY-mm-ddTHH:MM:SSZ). If time information is not available, 00:00:00 is used.
        """
        date_str = self._get_text_xpath(xpath)
        if date_str:
            try:
                datetime_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                datetime_obj = datetime.strptime(date_str, "%Y-%m-%d")
            formatted_date_str = datetime_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
            return formatted_date_str
        else:
            raise ValueError("No date found")

    @property
    def pid(self):
        """
        Return the PID for this record
        """
        try:
            return self._get_identifier("//cmd:Header/cmd:MdSelfLink/text()")
        except IndexError:
            # no PID found
            return None

    def check_pid_exists(self):
        """
        Only records with PIDs are relevant.
        """
        return bool(self.pid)

    def _get_list_of_licenses(self):
        """
        Retrieves all licenseInfo elements.
        """
        return self.xml.xpath(
            "//cmd:distributionInfo/cmd:licenceInfo", namespaces=self.namespaces
        )

    def check_resourcetype_corpus(self):
        """
        Helper method to only retrieve "corpus" records.
        """
        try:
            resourcetype = self._get_text_xpath("//cmd:resourceType/text()")
        except IndexError:
            # it seems that tool records have different location for resourceType?
            resourcetype = self._get_text_xpath("//oai:resourceType/text()")

        if resourcetype == "corpus":
            return True

    def _get_license_url_from_documentation(self):
        """
        Retrieves the license url.
        """

        doc_structured_elements = self.xml.xpath(
            "//cmd:resourceDocumentationInfo/cmd:documentationStructured/cmd:documentInfo",
            namespaces=self.namespaces,
        )

        for doc_element in doc_structured_elements:
            title_element = doc_element.xpath(
                "cmd:title[@xml:lang='en']/text()",
                namespaces=self.namespaces,
            )
            if title_element and "license" in title_element[0].lower():
                license_urn = doc_element.xpath(
                    "cmd:url/text()",
                    namespaces=self.namespaces,
                )
                if license_urn:
                    return license_urn[0]

        doc_unstructured_elements = self.xml.xpath(
            "//cmd:resourceDocumentationInfo/cmd:documentationUnstructured/cmd:documentUnstructured",
            namespaces=self.namespaces,
        )
        for doc_element in doc_unstructured_elements:
            doc_element_text = doc_element.text.strip().lower()
            if "license" in doc_element_text:
                license_urn = [
                    word
                    for word in doc_element_text.split()
                    if word.startswith("http://urn.fi")
                ]
                if license_urn:
                    return license_urn[0]

        return None

    def _get_license_information(self, license_element, mapped_licenses_dict):
        """
        Retrieves the license and its possible url to a dictionary.
        """
        license_dict = {}

        license_text = license_element.xpath(
            "cmd:licence/text()", namespaces=self.namespaces
        )[0]
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

        availability = self.xml.xpath(
            "//cmd:distributionInfo/cmd:availability/text()",
            namespaces=self.namespaces,
        )[0]
        if availability == "available-unrestrictedUse":
            access_type_dict["url"] = (
                "http://uri.suomi.fi/codelist/fairdata/access_type/code/open"
            )

        else:
            access_type_dict["url"] = (
                "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"
            )

        return access_type_dict

    def _map_access_rights(self):
        """
        Retrieves and maps all license and access type information to a dictionary.

        If any of the license elements found in the metadata do not have known specific
        uri.suomi.fi mapping, the said license is skipped. If this would result in no
        licenses being produced, the license is marked as "other".
        """
        license_package = {}
        license_mappings = {
            "CLARIN_PUB": "http://uri.suomi.fi/codelist/fairdata/license/code/ClarinPUB-1.0",
            "CLARIN_ACA": "http://uri.suomi.fi/codelist/fairdata/license/code/ClarinACA-1.0",
            "CLARIN_ACA-NC": "http://uri.suomi.fi/codelist/fairdata/license/code/ClarinACA+NC-1.0",
            "CLARIN_RES": "http://uri.suomi.fi/codelist/fairdata/license/code/ClarinRES-1.0",
            "other": "http://uri.suomi.fi/codelist/fairdata/license/code/other",
            "underNegotiation": "http://uri.suomi.fi/codelist/fairdata/license/code/undernegotiation",
            "proprietary": "http://uri.suomi.fi/codelist/fairdata/license/code/other-closed",
            "CC-BY": "http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-1.0",
            "CC-BY-ND": "http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-ND-4.0",
            "CC-BY-NC": "http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-NC-2.0",
            "CC-BY-SA": "http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-SA-3.0",
            "CC-BY-NC-ND": "http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-NC-ND-4.0",
            "CC-BY-NC-SA": "http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-NC-SA-4.0",
            "CC-ZERO": "http://uri.suomi.fi/codelist/fairdata/license/code/CC0-1.0",
            "ApacheLicence_2.0": "http://uri.suomi.fi/codelist/fairdata/license/code/Apache-2.0",
        }

        license_elements_list = self._get_list_of_licenses()
        license_list = []

        for license_element in license_elements_list:
            license = self._get_license_information(license_element, license_mappings)
            if license:
                license_list.append(license)

        if not license_list:
            license_list.append({"url": license_mappings["other"]})

        access_type = self._get_access_type()

        license_package["license"] = license_list
        license_package["access_type"] = access_type

        return license_package

    def _get_resource_languages(self):
        """
        Return the languages in the resource as a list of dicts containing lexvo urls.

        The input data from Metashare can contain both short 2-letter language codes
        (e.g. "fi") and the long 3-letter ones (e.g. "fin"), but Metax only accepts
        Lexvo URIs in form http://lexvo.org/id/iso639-3/LLL or similar for ISO 639-5.
        Here LLL is the three-letter language code from ISO 639-3. This requires
        translating the codes from one standard to another and prepending the lexvo URI.
        When available, ISO 639-3 is preferred.

        Same language code can be present in languages twice (e.g. fi for "Standard
        Finnish" and "Easy-to-read Finnish"), but those are eliminated.
        """
        language_codes = self.xml.xpath(
            "//cmd:languageInfo/cmd:languageId/text()", namespaces=self.namespaces
        )
        iso639_urls = set()
        for language_code in language_codes:
            try:
                language = iso639.Lang(language_code.lower())

                if language.pt3:
                    language_uri = f"http://lexvo.org/id/iso639-3/{language.pt3}"
                elif language.pt5:
                    language_uri = f"http://lexvo.org/id/iso639-5/{language.pt5}"
                else:
                    raise ValueError(
                        "Could not determine three-letter language code for %s"
                        % language_code,
                    )

                if language_validator.language_in_vocabulary(language_uri):
                    iso639_urls.add(language_uri)

            except iso639.exceptions.InvalidLanguageValue:
                raise ValueError(
                    "Could not determine ISO 639 language code for %s" % language_code,
                )

        return [{"url": url} for url in iso639_urls]

    def _get_actors(self):
        """
        Return the actors for this resource.
        """
        actors = []

        actor_role_element_xpaths = {
            "creator": "//cmd:metadataInfo/cmd:metadataCreator",
            "publisher": "//cmd:distributionInfo/cmd:licenceInfo/cmd:distributionRightsOrganization",
            "curator": "//cmd:resourceInfo/cmd:contactPerson",
            "rights_holder": "//cmd:distributionInfo/cmd:iprHolderPerson",
        }

        for role, xpath in actor_role_element_xpaths.items():
            curator_elements = self.xml.xpath(xpath, namespaces=self.namespaces)

            if not isinstance(curator_elements, list):
                curator_elements = [curator_elements]

            for curator_element in curator_elements:
                new_actor = Actor(curator_element, roles=[role])
                if not new_actor.has_person_data:
                    continue
                if new_actor in actors:
                    actors[actors.index(new_actor)].add_roles(new_actor.roles)
                else:
                    actors.append(new_actor)

        return [actor.to_metax_dict() for actor in actors]

    def to_dict(self):
        """
        Converts text and dictionaries to Metax compliant dictionary.
        """
        return {
            "data_catalog": "urn:nbn:fi:att:data-catalog-kielipankki",
            "language": self._get_resource_languages(),
            "field_of_science": [
                {
                    "url": "http://www.yso.fi/onto/okm-tieteenala/ta6121",
                }
            ],
            "persistent_identifier": self.pid,
            "title": self._get_element_text_in_preferred_language("//cmd:resourceName"),
            "description": self._get_element_text_in_preferred_language(
                "//cmd:description"
            ),
            "modified": self._get_datetime("oai:header/oai:datestamp/text()"),
            "created": self._get_datetime("//cmd:Header/cmd:MdCreationDate/text()"),
            "access_rights": self._map_access_rights(),
            "actors": self._get_actors(),
        }
