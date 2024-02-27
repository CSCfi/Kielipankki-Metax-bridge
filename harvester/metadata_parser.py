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
                f"{xpath}[@lang='{lang}']/text()",
                namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"},
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
        return self.xml.xpath(
            xpath, namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"}
        )[0]

    def _get_identifier(self, xpath):
        """
        Retrieves the urn of the given XPath's url.
        """
        identifier_url = self._get_text_xpath(xpath).strip()
        netloc, path = urlparse(identifier_url).netloc, urlparse(identifier_url).path
        return netloc + path

    def _get_datetime(self, xpath):
        """
        Retrieve the datetime from given XPath as a string (YYYY-mm-ddTHH:MM:SSZ)
        """
        date_str = self._get_text_xpath(xpath)
        if date_str:
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
            return self._get_identifier(
                "//info:identificationInfo/info:identifier/text()",
            )
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
            "//info:distributionInfo/info:licenceInfo",
            namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"},
        )

    def check_resourcetype_corpus(self):
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
        doc_elements = self.xml.xpath(
            "//info:resourceDocumentationInfo/info:documentation",
            namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"},
        )

        for doc_element in doc_elements:
            doc_unstruct_element = doc_element.xpath(
                "info:documentUnstructured/text()",
                namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"},
            )
            doc_info_elements = doc_element.xpath(
                "info:documentInfo",
                namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"},
            )

            if doc_unstruct_element:
                doc_unstruct_text = doc_unstruct_element[0].lower()
                if "license:" in doc_unstruct_text or "license" in doc_unstruct_text:
                    license_urn = [
                        word
                        for word in doc_unstruct_text.split()
                        if word.startswith("http://urn.fi")
                    ]
                    if license_urn:
                        return license_urn[0]

            elif doc_info_elements:
                for doc_info_elem in doc_info_elements:
                    title_element = doc_info_elem.xpath(
                        "info:title[@lang='en']/text()",
                        namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"},
                    )
                    if title_element and "license" in title_element[0].lower():
                        license_urn = doc_info_elem.xpath(
                            "info:url/text()",
                            namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"},
                        )
                        if license_urn:
                            return license_urn[0]

        return None

    def _get_license_information(self, license_element, mapped_licenses_dict):
        """
        Retrieves the license and its possible url to a dictionary.
        """
        license_dict = {}

        license_text = license_element.xpath(
            "info:licence/text()",
            namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"},
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
            "//info:distributionInfo/info:availability/text()",
            namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"},
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

        We also have two non-standard language codes in the source data: "hbp" and "hbk"
        for "Hungarian (Budapest)" and "Hungarian (Bucharest)". These are submitted as
        plain Hungarian (hun).

        Same language code can be present in languages twice (e.g. fi for "Standard
        Finnish" and "Easy-to-read Finnish"), but those are eliminated.
        """
        language_codes = self.xml.xpath(
            "//info:languageInfo/info:languageId/text()",
            namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"},
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
                if language_code in ["hbk", "hbp"]:
                    iso639_urls.add("http://lexvo.org/id/iso639-3/hun")
                else:
                    raise ValueError(
                        "Could not determine ISO 639 language code for %s"
                        % language_code,
                    )

        return [{"url": url} for url in iso639_urls]

    def _get_actors(self):
        """
        Return the actors for this resource.
        """
        actors = []

        actor_role_element_xpaths = {
            "creator": "//info:metadataInfo/info:metadataCreator",
            "publisher": "//info:distributionInfo/info:licenceInfo/info:distributionRightsHolder",
            "curator": "//info:resourceInfo/info:contactPerson",
            "rights_holder": "//info:distributionInfo/info:iprHolder",
        }

        for role, xpath in actor_role_element_xpaths.items():
            curator_elements = self.xml.xpath(
                xpath,
                namespaces={"info": "http://www.ilsp.gr/META-XMLSchema"},
            )

            if not isinstance(curator_elements, list):
                curator_elements = [curator_elements]

            for curator_element in curator_elements:
                new_actor = Actor(curator_element, roles=[role])
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
            "title": self._get_element_text_in_preferred_language(
                "//info:resourceName"
            ),
            "description": self._get_element_text_in_preferred_language(
                "//info:description"
            ),
            "modified": self._get_datetime(
                "//info:metadataInfo/info:metadataLastDateUpdated/text()"
            ),
            "created": self._get_datetime(
                "//info:metadataInfo/info:metadataCreationDate/text()"
            ),
            "access_rights": self._map_access_rights(),
            "actors": self._get_actors(),
        }
