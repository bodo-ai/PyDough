"""
Base definition of PyDough metadata for template attributes.
"""

from pydough.errors.error_types import PyDoughMetadataException
from pydough.errors.error_utils import (
    HasType,
    attributes_usage_predicate,
    extract_array,
    extract_integer,
    extract_object,
    extract_string,
    is_integer,
    is_string,
)
from pydough.metadata.abstract_metadata import AbstractMetadata
from pydough.metadata.graphs import GraphMetadata


class AttributeMetadata(AbstractMetadata):
    """
    Concrete metadata implementation class for PyDough template attributes.
    Representing the options (labels and values) that can be used on the templates
    definitions.
    """

    def __init__(
        self,
        name: str,
        graph: GraphMetadata,
        usage: dict[str, list[str]],
        type: str,
        description: str,
    ):
        HasType(GraphMetadata).verify(graph, f"graph {name!r}")

        self._graph: GraphMetadata = graph
        self._name: str = name
        self._usage: dict[str, list[str]] = usage
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
    def usage(self) -> dict[str, list[str]]:
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
        return f"{self.graph.path}.templates.attributes.{self.name}"

    @staticmethod
    def create_error_name(name: str, graph_error_name: str):
        return f"template attribute {name!r} in {graph_error_name}"

    def add_attribute_option(self, label: str, value: str | int) -> None:
        """
        Add an option to the list of options
        """
        self.options[label] = value

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
        attr_usage: dict[str, list[str]] = extract_object(
            attribute_json, "usage", error_name
        )
        attributes_usage_predicate.verify(attr_usage, error_name)

        attr_type: str = extract_string(attribute_json, "type", error_name)
        # Validate the type of the attribute
        if not graph.is_valid_data_type(attr_type):
            raise PyDoughMetadataException(
                f"Invalid type {attr_type!r} for attribute {attribute_name!r} in graph {graph.name!r}. Must be one of: {sorted(graph.ALLOWED_TYPES)}"
            )

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

        if len(attr_options) == 0:
            raise PyDoughMetadataException(
                f"Template attribute {attribute_name!r} in graph {graph.name!r} "
                f"must have at least one option defined."
            )

        option_error_name: str = (
            f"Option in attribute {attribute_name!r} in graph {graph.name!r}"
        )

        # Determine whether all option values are consistently strings or consistently
        # integers -- reject any mix, and reject any other type entirely.
        option_values = [option.get("value") for option in attr_options]
        all_strings = all(is_string.accept(v) for v in option_values)
        all_integers = all(is_integer.accept(v) for v in option_values)

        if not (all_strings or all_integers):
            raise PyDoughMetadataException(
                f"{option_error_name} 'value' fields must be either all strings or "
                f"all integers (not a mix, and no other type)."
            )

        for option in attr_options:
            label: str = extract_string(option, "label", option_error_name)
            value: str | int = (
                extract_string(option, "value", option_error_name)
                if all_strings
                else extract_integer(option, "value", option_error_name)
            )

            graph_labels: dict[str, str] = graph.get_all_labels()
            if label in graph_labels or label in new_attribute.options:
                by_attribute: str = (
                    graph_labels[label] if label in graph_labels else attribute_name
                )
                raise PyDoughMetadataException(
                    f"Duplicate option label: {label!r} for attribute {attribute_name!r}. "
                    f"The label is already in use by attribute {by_attribute!r} "
                    f"in graph {graph.name!r}."
                )
            new_attribute.add_attribute_option(label, value)

        graph.add_template_attribute(new_attribute)
