"""
Base definition of PyDough metadata for template attributes.
"""

from pydough.errors.error_utils import (
    HasType,
    extract_array,
    extract_integer,
    extract_string,
)
from pydough.metadata.abstract_metadata import AbstractMetadata
from pydough.metadata.graphs import GraphMetadata


class AttributeMetadata(AbstractMetadata):
    """
    Abstract base class for PyDough metadata for template attributes.
    """

    # Set of names of fields that can be included in the JSON
    # object describing a template attribute. Implementations should extend this.
    allowed_fields: set[str] = {
        "name",
        "usage",
        "type",
        "description",
        "options",
    }

    def __init__(
        self,
        name: str,
        graph: GraphMetadata,
        usage: list[str],
        type: str,
        description: str,
    ):
        # TODO: Check if the name is a valid one (not a the graph, collection name)
        # Really this name is not going to be use for other than just identifying
        # the attribute so maybe is not really worth the check
        HasType(GraphMetadata).verify(graph, f"graph {name!r}")

        self._graph: GraphMetadata = graph
        self._name: str = name
        self._usage: list[str] = usage
        self._type: str = type
        self._options: dict[str, str | int] = {}

        super().__init__(description, None, None)

    @property
    def graph(self) -> GraphMetadata:
        """
        The graph that the template attribute belongs to.
        """
        return self._graph

    @property
    def name(self) -> str:
        """
        The name of the template attribute
        """
        return self._name

    @property
    def usage(self) -> list[str]:
        """
        List with the names of the templates where the attribute can be used
        """
        return self._usage

    @property
    def type(self) -> str:
        """
        Type of the data saved on the options for the attribute.

        NOTE: `'pydough'` is a special type to identify if the value is a pydough
        expression.
        """

        return self._type

    @property
    def options(self) -> dict[str, str | int]:
        """
        List with all options of the attribute
        """
        return self._options

    @property
    def error_name(self):
        return self.create_error_name(self.name, self.graph.error_name)

    @property
    def components(self):
        comp: list = [self.name, self.description, self.type]
        comp.extend(self.usage)
        return comp

    @property
    def path(self) -> str:
        return f"{self.graph.path}.{self.name}"

    def add_attribute_option(self, label: str, value: str | int) -> None:
        """
        Add an option to the list of options
        """
        if label in self.options:
            raise ValueError(f"Duplicate option label: {label!r}")

        self.options[label] = value

    @staticmethod
    def create_error_name(name: str, graph_error_name: str):
        return f"template attribute {name!r} in {graph_error_name}"

    def verify_complete(self) -> None:
        """
        Verifies that a template attribute is well-formed after the parsing of all of
        its properties is complete. Subclasses should extend the checks done
        in the default implementation.

        Raises:
            `PyDoughMetadataException`: if the template attribute is malformed
            in any way after parsing is done.
        """
        # TODO
        return

    @staticmethod
    def parse_from_json(
        graph: GraphMetadata, attribute_name: str, attribute_json: dict
    ) -> None:
        """
        Parses a JSON object into the metadata for a template attribute
        and inserts it into the graph.

        Args:
            `graph`: the metadata for the graph that the template attribute will
            be added to.
            `attribute_name`: the name of the template attribute that will be
            added to the graph.
            `attribute_json`: the JSON object that is being parsed to create
            the new template attribute.

        Raises:
            `PyDoughMetadataException`: if the JSON does not meet the necessary
            structure properties.
        """

        error_name: str = AttributeMetadata.create_error_name(
            attribute_name, graph.error_name
        )

        # Extract the relevant properties from the JSON to build the new template
        # attribute, then add it to the graph
        attr_usage: list[str] = extract_array(attribute_json, "usage", error_name)
        attr_type: str = extract_string(attribute_json, "type", error_name)
        attr_desc: str = extract_string(attribute_json, "description", error_name)

        new_attribute: AttributeMetadata = AttributeMetadata(
            attribute_name,
            graph,
            attr_usage,
            attr_type,
            attr_desc,
        )

        # Parse and add the options
        attr_options: list = extract_array(attribute_json, "options", error_name)

        for option in attr_options:
            label: str = extract_string(option, "label", error_name)
            value: str | int
            try:
                value = extract_string(option, "value", error_name)
            except AssertionError:
                value = extract_integer(option, "value", error_name)

            new_attribute.add_attribute_option(label, value)

        graph.add_template_attribute(new_attribute)
