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
        return {"roles": sorted(list(self.roles)), "person": self._person_dict}

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

    def __eq__(self, other):
        """
        Check if two objects represent the same person.

        The actors are deemed equal if their names and emails match. This allows merging
        the entries of same person having multiple roles.
        """
        if not isinstance(other, Actor):
            return False

        return self.name == other.name and self.email == other.email
