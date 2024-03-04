import re


class Actor:
    def __init__(self, element, roles):
        """
        Create a new actor.

        Thea actor data is parsed from the input lxml Element into a dict for easier
        use. The root element (e.g. "contactPerson") is omitted, because that
        information is already included in `roles`.
        """
        self.data = self._etree_to_dict(element)
        if len(self.data) == 1:
            self.data = list(self.data.values())[0]

        self.roles = set(roles)

    def _etree_to_dict(self, element):
        result = {}
        key = re.sub("{.*}", "", element.tag)
        if len(element) == 0:
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
        Return
        """
        if "givenName" in self.data:
            return f"{self.data['givenName']} {self.data['surname']}"
        elif "surname" in self.data:
            return f"{self.data['surname']}"
        else:
            return None

    @property
    def email(self):
        """
        Email address of the person. None if not available.
        """
        communicationInfo = self.data.get("communicationInfo", {})
        return communicationInfo.get("email", None)

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
        return {
            "roles": sorted(list(self.roles)),
            "person": self._person_dict,
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
        else:
            return None

    @property
    def has_person_data(self):
        """
        Return True if the actor has information about an individual person.
        """
        return self._person_dict is not None

    @property
    def _organization_url(self):
        """
        Return URI for the affiliation of this actor.

        The URIs are from http://uri.suomi.fi/codelist/fairdata/organization/code. For
        FIN-CLARIAH affiliations, the home organization from the `departmentName` field
        is used when determining the URI.

        Raises UnknownOrganizationException if URI match is not found.
        """

        organization_name = self.data["affiliation"]["organizationName"]

        if organization_name == "FIN-CLARIN":
            organization_name = self.data["affiliation"]["departmentName"]

        url_base = "http://uri.suomi.fi/codelist/fairdata/organization/code"
        organization_codes = {
            "Aalto University": "10076",
            "CSC — IT Center for Science Ltd": "09206320",
            "Centre for Applied Language Studies": "01906-213060",
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

    @property
    def _organization_dict(self):
        """
        Return organization information about the actor as a Metax-compatible dict.

        If the actor is not an organization or a person with organization information
        available, None is returned.
        """
        if "affiliation" not in self.data:
            return None

        try:
            return {"url": self._organization_url}
        except UnknownOrganizationException as e:
            # TODO: this should be eliminated before this ticket is done
            print(e)
            return None

    def __eq__(self, other):
        """
        Check if two objects represent the same person.

        The actors are deemed equal if their names and emails match. This allows merging
        the entries of same person having multiple roles.
        """
        if not isinstance(other, Actor):
            return False

        return self.name == other.name and self.email == other.email


class UnknownOrganizationException(Exception):
    """
    Exception to be raised when an URI cannot be determined for an organization
    """
