import re


class Actor:
    def __init__(self, element, roles):
        """
        Create a new actor.

        Thea actor data is parsed from the input lxml Element into a dict for easier
        use. The root element (e.g. "contactPerson") is omitted, because that
        information is already included in `roles`.
        """
        self.supported_languages = ["en", "fi", "und"]
        self.data = self._etree_to_dict(element)
        if len(self.data) == 1:
            self.data = list(self.data.values())[0]

        self.roles = set(roles)

    def _etree_to_dict(self, element):
        """
        Convert the XML data describing this actor into a dict

        If there are fields with language information (e.g. organization names, but also
        person names), language information is added to the field name, separated by an
        underscore.

        NB: if a field is repeated without changing language information (e.g. multiple
        email addresses), only the last one will be present in the dict.

        The resulting dict can e.g. look something like this:
        {
          "contactPerson": {
            "surname_en": "Smith",
            "givenName_en": "John",
            "sex": "female",
            "communicationInfo": {
              "email": "john.smith@example.com",
              "country": "Finland"
            },
            "affiliation": {
              "organizationName_en": "University of Whatnot",
              "departmentName_en": "Department of Science",
              "organizationName_fi": "Joku Yliopisto",
              "departmentName_fi": "Tieteen osasto",
              "communicationInfo": {
                "email": "whatnot_univ@example.com",
                "country": "Finland"
              }
            }
          }
        }

        """
        result = {}
        key = re.sub("{.*}", "", element.tag)
        if len(element) == 0:
            languages = element.xpath("@xml:lang")
            if languages:
                key = key + "_" + languages[0]
            result[key] = element.text
        else:
            subresult = {}
            for child in element:
                subresult.update(self._etree_to_dict(child))
            result[key] = subresult

        return result

    @property
    def name(self):
        """
        Return name of the person represented by this actor.

        If the name is provided in more than one language, only one is returned,
        preference order being determined by the order of `self.supported_languages`.
        """
        if "personInfo" not in self.data:
            return None

        person_info = self.data["personInfo"]
        for language in self.supported_languages:
            if f"givenName_{language}" in person_info:
                return f"{person_info['givenName'+'_'+language]} {person_info['surname'+'_'+language]}"
            if f"surname_{language}" in person_info:
                return f"{person_info['surname'+'_'+language]}"
        return None

    @property
    def email(self):
        """
        Email address of the person. None if not available.
        """
        communication_info = self.data["personInfo"].get("communicationInfo", {})
        return communication_info.get("email", None)

    def add_roles(self, roles):
        """
        Add given roles for this actor.
        """
        self.roles.update(roles)

    def to_metax_dict(self):
        """
        Return the actor as a Metax-compatible dict

        The list of roles is sorted to make sure that the representation for a same
        actor always stays the same. In addition to just being neat, this also improves
        testaibility. Performance hit should be minimal, as the lists are tiny (4 items
        at most).
        """
        if self.has_person_data:
            return {
                "roles": sorted(self.roles),
                "person": self._person_dict,
                "organization": self._organization_dict,
            }

        return {
            "roles": sorted(self.roles),
            "organization": self._organization_dict,
        }

    @property
    def _person_dict(self):
        """
        Return personal information about the actor as a Metax-compatible dict.

        If the actor is not a person or personal information is otherwise not available,
        None is returned.
        """
        if self.name:
            return {"name": self.name, "email": self.email}
        return None

    @property
    def has_person_data(self):
        """
        Return True if the actor has information about an individual person.
        """
        return self._person_dict is not None

    @property
    def _organization_uri(self):
        """
        Return URI for the affiliation of this actor.

        The URIs are from http://uri.suomi.fi/codelist/fairdata/organization/code. For
        FIN-CLARIAH affiliations, the home organization from the `departmentName` field
        is used when determining the URI.

        Raises UnknownOrganizationException if URI match is not found.
        """
        if "organizationName_en" in self._organization_data["organizationInfo"]:
            organization_name = self._organization_data["organizationInfo"][
                "organizationName_en"
            ]
        elif "organizationName_fi" in self._organization_data["organizationInfo"]:
            organization_name = self._organization_data["organizationInfo"][
                "organizationName_fi"
            ]
        else:
            raise UnableToParseOrganizationInfoException(
                "Could not determine organization name from "
                f"{self._organization_data['organizationInfo']}"
            )

        url_base = "http://uri.suomi.fi/codelist/fairdata/organization/code"
        organization_codes = {
            "Aalto University": "10076",
            "CSC — IT Center for Science Ltd": "09206320",
            "Centre for Applied Language Studies": "01906-213060",
            "FIN-CLARIN": "01901",
            "National Library of Finland": "01901-H981",
            "South Eastern Finland University of Applied Sciences": "10118",
            "University of Eastern Finland": "10088",
            "University of Helsinki": "01901",
            "University of Jyväskylä": "01906",
            "University of Oulu": "01904",
            "University of Tampere": "10122",
            "University of Turku": "10089",
        }
        if organization_name in organization_codes:
            return f"{url_base}/{organization_codes[organization_name]}"
        raise UnknownOrganizationException(
            f"Could not determine URI for {organization_name}"
        )

    def _none_if_person_witout_affiliation(func):
        """
        Wrapper for properties that are None if the actor does not have
        affiliation/organization data.
        """
        # Wrapper shouldn't have self despite being defined in class
        # pylint: disable=no-self-argument

        def wrapper(self, *args, **kwargs):
            if (
                "organizationInfo" not in self.data
                and "affiliation" not in self.data["personInfo"]
                and "organizationName" not in self.data
            ):
                return None
            # pylint: disable=not-callable
            return func(self, *args, **kwargs)

        return wrapper

    @property
    @_none_if_person_witout_affiliation
    def _organization_data(self):
        if "personInfo" in self.data and "affiliation" in self.data["personInfo"]:
            return self.data["personInfo"]["affiliation"]
        elif "affiliation" in self.data:
            return self.data["affiliation"]
        else:
            return self.data

    @property
    @_none_if_person_witout_affiliation
    def organization_name(self):
        """
        Return the names of the organizaton in all supported and provided languages.

        The element is mandatory in the profile we use, so handling for missing key is
        not needed, but there can be multiple language versions, each of which is sent
        to Metax.
        """

        languages = {}
        for language in self.supported_languages:
            if (
                f"organizationName_{language}"
                in self._organization_data["organizationInfo"]
            ):
                languages[language] = self._organization_data["organizationInfo"][
                    f"organizationName_{language}"
                ]
        return languages

    @property
    @_none_if_person_witout_affiliation
    def organization_homepage(self):
        """
        Return the organization home page as Metax-compatible dict if available, otherwise None.

        The communicationInfo element is mandatory for affiliation, so handling for that
        not being present is not needed.
        """
        if "url" in self._organization_data["organizationInfo"]["communicationInfo"]:
            return {
                "url": self._organization_data["organizationInfo"]["communicationInfo"][
                    "url"
                ]
            }
        return None

    @property
    @_none_if_person_witout_affiliation
    def organization_email(self):
        """
        Return the organization's contact email.

        The communicationInfo and email elements are mandatory, so they should always be
        present.
        """
        return self._organization_data["organizationInfo"]["communicationInfo"]["email"]

    @property
    @_none_if_person_witout_affiliation
    def _organization_dict(self):
        """
        Return organization information about the actor as a Metax-compatible dict.

        If the actor is not an organization or a person with organization information
        available, None is returned.
        """
        try:
            return {"url": self._organization_uri}
        except UnknownOrganizationException:
            return {
                "pref_label": self.organization_name,
                "homepage": self.organization_homepage,
                "email": self.organization_email,
            }

    def __eq__(self, other):
        """
        Check if two objects represent the same person.

        The actors are deemed equal if their names, emails and organizations match. This
        allows merging the entries of same person having multiple roles. The same
        natural person having represented different organizations will not be merged.
        """
        if not isinstance(other, Actor):
            return False

        if self.has_person_data:
            return (
                self.name == other.name
                and self.email == other.email
                and self._organization_dict == other._organization_dict
            )

        return (
            self._organization_dict == other._organization_dict
            and not other.has_person_data
        )


class UnknownOrganizationException(Exception):
    """
    Exception to be raised when an URI cannot be determined for an organization
    """


class UnableToParseOrganizationInfoException(Exception):
    """
    Exception to be raised when some information for an organization cannot be parsed
    """
