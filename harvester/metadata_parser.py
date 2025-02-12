from datetime import datetime
from urllib.parse import urlparse

from lxml import etree
import iso639

from harvester.actor import (
    Actor,
    UnknownOrganizationException,
    UnableToParseOrganizationInfoException,
)
from harvester import language_validator


class RecordParsingError(Exception):
    """
    Exception to be raised when all required information cannot be parsed from a record.
    """

    def __init__(self, message, identifier):
        super().__init__(message)
        self.message = message
        self.identifier = identifier

    def __str__(self):
        return f"Error parsing record {self.identifier}: {self.message}"


class RecordParser:
    def __init__(self, xml):
        """
        Create a CMDI record object.

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
            raise RecordParsingError(f"No date found from string {date_str}", self.pid)

    @property
    def pid(self):
        """
        Return the PID for this record
        """
        try:
            return self._get_text_xpath("//cmd:Header/cmd:MdSelfLink/text()").strip()
        except IndexError:
            comedi_identifier = self._get_text_xpath(
                "//oai:header/oai:identifier/text()"
            )
            raise RecordParsingError("Could not determine PID", comedi_identifier)

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
                    if "://urn.fi/urn:nbn:fi" in word
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

        Resources that are not open must have a reason for their restrictions. For ACA
        resources, we select the research use restriction, but unfortunately it is not
        simple to determine the underlying reason for other resources based on their
        metadata, so we simply specify "other". If the access type is open though, we
        must not provide any restriction reasons, even if the license suggests that this
        is an ACA resource.
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
        restriction_grounds_urls = set()

        for license_element in license_elements_list:
            license = self._get_license_information(license_element, license_mappings)
            if license:
                license_list.append(license)
                if "ClarinACA" in license["url"]:
                    restriction_grounds_urls.add(
                        "http://uri.suomi.fi/codelist/fairdata/restriction_grounds/code/research"
                    )

        if not license_list:
            license_list.append({"url": license_mappings["other"]})

        access_type = self._get_access_type()
        if (
            access_type["url"]
            == "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"
        ) and not restriction_grounds_urls:
            restriction_grounds_urls.add(
                "http://uri.suomi.fi/codelist/fairdata/restriction_grounds/code/other"
            )

        license_package["license"] = license_list
        license_package["access_type"] = access_type
        if restriction_grounds_urls:
            license_package["restriction_grounds"] = [
                {"url": url} for url in restriction_grounds_urls
            ]

        return license_package

    def _get_resource_languages(self):
        """
        Return the languages in the resource as a list of dicts containing lexvo urls.

        The input data from Comedi can contain both short 2-letter language codes
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
                    raise RecordParsingError(
                        f"Could not determine three-letter language code for {language_code}",
                        self.pid,
                    )

                if language_validator.language_in_vocabulary(language_uri):
                    iso639_urls.add(language_uri)

            except iso639.exceptions.InvalidLanguageValue:
                raise RecordParsingError(
                    f"Could not determine ISO 639 language code for {language_code}",
                    self.pid,
                )

        return [{"url": url} for url in iso639_urls]

    def _get_actors(self):
        """
        Return the actors for this resource.

        Metax requires at least one creator and exactly one publisher for each dataset,
        while Comedi does not enforce any limits on these, so an error is raised if
        these are not found in the source data.

        NB: due to Metax requiring there to be exactly one publisher for each dataset,
        we need to adjust some actor sets.
        """
        actors = []

        actor_role_element_xpaths = {
            "creator": ["//cmd:metadataInfo/cmd:metadataCreator"],
            "publisher": [
                "//cmd:distributionInfo/cmd:licenceInfo/cmd:distributionRightsHolderPerson",
                "//cmd:distributionInfo/cmd:licenceInfo/cmd:distributionRightsHolderOrganization",
            ],
            "curator": ["//cmd:resourceInfo/cmd:contactPerson"],
            "rights_holder": [
                "//cmd:distributionInfo/cmd:iprHolderPerson",
                "//cmd:distributionInfo/cmd:iprHolderOrganization",
            ],
        }

        for role, xpaths in actor_role_element_xpaths.items():
            for xpath in xpaths:
                curator_elements = self.xml.xpath(xpath, namespaces=self.namespaces)

                if not isinstance(curator_elements, list):
                    curator_elements = [curator_elements]

                for curator_element in curator_elements:
                    new_actor = Actor(curator_element, roles=[role])
                    if not new_actor.organization_name:
                        raise RecordParsingError(
                            f"Could not find affiliation for {new_actor.name}", self.pid
                        )

                    try:
                        if new_actor in actors:
                            actors[actors.index(new_actor)].add_roles(new_actor.roles)
                        else:
                            actors.append(new_actor)
                    except (
                        UnknownOrganizationException,
                        UnableToParseOrganizationInfoException,
                    ) as err:
                        raise RecordParsingError(str(err), self.pid)

        if sum(1 for actor in actors if "creator" in actor.roles) == 0:
            raise RecordParsingError(
                "No metadata creators (creator in Metax) found", self.pid
            )

        publisher_actors = sum(1 for actor in actors if "publisher" in actor.roles)
        if publisher_actors == 0:
            raise RecordParsingError(
                "No distribution rightsholders (publisher in Metax) found",
                self.pid,
            )

        actor_dicts = [actor.to_metax_dict() for actor in actors]

        if publisher_actors > 1:
            actor_dicts = self._replace_multiple_publishers_with_explanation(
                actor_dicts
            )

        return actor_dicts

    def _replace_multiple_publishers_with_explanation(self, actor_dicts):
        """
        When run against an otherwise Metax-ready actor dict, this returns a new dict
        with otherwise the same actors but with all real publisher actors removed and
        a dummy one added in their stead, letting the reader know that they need to visit
        the original metadata for accurate information.
        """
        cleaned_actor_dicts = []
        for actor in actor_dicts:
            if "publisher" in actor["roles"]:
                actor["roles"].remove("publisher")
            if actor["roles"]:
                cleaned_actor_dicts.append(actor)

        cleaned_actor_dicts.append(
            {
                "roles": ["publisher"],
                "organization": {
                    "pref_label": {
                        "en": "Multiple publishers, check distribution rights holders "
                        "in original metadata by following its persistent identifier"
                    }
                },
            }
        )

        return cleaned_actor_dicts

    def to_dict(self, data_catalog):
        """
        Converts text and dictionaries to Metax compliant dictionary.

        :data_catalog: Metax data catalog to which this record will be sent
        """
        return {
            "data_catalog": data_catalog,
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
            "state": "published",
        }
