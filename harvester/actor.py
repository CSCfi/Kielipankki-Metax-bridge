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

    def to_metax_dict(self):
        if self.name:
            person_dict = {"name": self.name, "email": self.email}
        else:
            person_dict = None
        return {"roles": list(self.roles), "person": person_dict}
