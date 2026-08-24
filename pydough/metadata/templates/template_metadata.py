"""
Base definition of PyDough metadata for template definition.
"""

import ast
import re
import textwrap
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pydough.errors.error_types import PyDoughMetadataException
from pydough.errors.error_utils import extract_object, extract_string
from pydough.metadata.abstract_metadata import AbstractMetadata
from pydough.metadata.graphs.graph_metadata import GraphMetadata


@dataclass
class TemplateParameter:
    """
    Dataclass contaning everything needed for the template parameter
    """

    name: str
    """
    Name of the parameter
    """

    type: str
    """
    String containing type of data this parameter receives.
    NOTE: `pydough` type will keep this parameter without type hint for the
    template.
    """

    description: str
    """
    Detail of what this parameter represents inside the template or how it is used
    """


class TemplateMetadata(AbstractMetadata):
    """
    Concrete metadata implementation class for PyDough template definitions.
    """

    def __init__(
        self,
        name: str,
        graph: GraphMetadata,
        description: str,
        parameters: dict[str, dict[str, str]],
        code: str,
        answer_variable: str = "result",
    ):
        super().__init__(description, None, None)

        self._graph: GraphMetadata = graph
        self._name: str = name
        self._code: str = code
        self._answer_variable: str = answer_variable

        self._parameters: dict[str, TemplateParameter] = (
            TemplateMetadata.parse_parameters_from_json(parameters)
        )
        self._template_callable: Callable = self.create_template_callable(graph)

    @property
    def graph(self) -> GraphMetadata:
        """
        The graph that the template belongs to.
        """
        return self._graph

    @property
    def name(self) -> str:
        """
        Name of the template.
        """
        return self._name

    @property
    def parameters(self) -> dict[str, TemplateParameter]:
        """
        Parameters required for this template.
        """
        return self._parameters

    @property
    def code(self) -> str:
        """
        PyDough code being return when the template is called.
        """
        return self._code

    @property
    def answer_variable(self) -> str:
        """
        Name of the variable being returned and holds the final answer.
        """
        return self._answer_variable

    @property
    def template_callable(self) -> Callable:
        """
        Template callable used for its execution.
        """
        return self._template_callable

    @property
    def error_name(self):
        return self.create_error_name(self.name, self.graph.error_name)

    @property
    def components(self):
        comp: list = [self.name, self.description, self.code]
        return comp

    @property
    def path(self) -> str:
        # TODO: Not sure about the usage of this path
        return f"{self.graph.path}.templates.definitions.{self.name}"

    @staticmethod
    def create_error_name(name: str, graph_error_name: str):
        return f"template definition {name!r} in {graph_error_name}"

    @staticmethod
    def parse_from_json(graph: GraphMetadata, name: str, definition_json: dict) -> None:
        """
        Parses a JSON object into the metadata for a template definition
        and inserts it into the graph.

        Args:
            `graph`: the metadata for the graph that the template attribute will
            be added to.
            `name`: the name of the template definition that will be
            added to the graph.
            `definition_json`: the JSON object that is being parsed to create
            the new template definition.

        Raises:
            `PyDoughMetadataException`: if the JSON does not meet the necessary
            structure properties.
        """
        description: str = extract_string(
            definition_json, "description", graph.error_name
        )
        answer_variable: str = extract_string(
            definition_json, "answer_variable", graph.error_name
        )
        code: str = extract_string(definition_json, "code", graph.error_name)
        kwargs: dict[str, dict] = extract_object(
            definition_json, "parameters", graph.error_name
        )

        new_template: TemplateMetadata = TemplateMetadata(
            name,
            graph,
            description,
            kwargs,
            code,
            answer_variable,
        )

        graph.add_template_definition(name, new_template)

    @staticmethod
    def parse_parameters_from_json(
        parameters_json: dict,
    ) -> dict[str, TemplateParameter]:
        """
        Parses a JSON object into the parameters for a template definition
        and returns it

        Args:
            `parameters_json`: the JSON object that is being parsed to create
            the parameters required for a template.

        Returns:
            All paremeters parsed for a template.

        Raises:
            `PyDoughMetadataException`: if the JSON does not meet the necessary
            structure properties.
        """

        template_params: dict[str, TemplateParameter] = {}

        for param_name, arg in parameters_json.items():
            if param_name in template_params:
                raise PyDoughMetadataException(
                    f"Already added {param_name} to the template's parameters"
                )

            param_type: str = extract_string(
                arg, "type", "All parameters must have type"
            )
            param_description: str = extract_string(
                arg, "description", "All parameters must have description"
            )
            new_param = TemplateParameter(param_name, param_type, param_description)

            template_params[param_name] = new_param

        return template_params

    def create_template_callable(
        self,
        graph: GraphMetadata | None = None,
    ) -> Callable:
        """
        Builds a callable from a PyDough source string, without executing it.
        Intended for constructing template functions that get stored (e.g. via
        `graph.add_template_definition`) and invoked later.

        Args:
            `graph`: the metadata graph bound into the function's closure as
            `_graph`.

        Returns:
            The callable built from `source`, not yet invoked.
        """

        template_str: str = self.create_template_def()

        import pydough
        from pydough.unqualified.unqualified_transform import AddRootVisitor

        # Args + "pydough" are "known" so they aren't
        # rewritten into _ROOT.<name> by the visitor.
        known_names: set[str] = {"pydough"}
        graph_name: str = "_graph"
        visitor = AddRootVisitor(graph_name, known_names)

        tree: ast.AST = ast.parse(template_str)
        new_tree: ast.AST = ast.fix_missing_locations(visitor.visit(tree))
        transformed_code: str = ast.unparse(new_tree)

        compiled = compile(transformed_code, filename=f"<{self.name}>", mode="exec")

        # `_graph` and `pydough` are baked into the function's globals so that
        # they're available whenever the function is later called
        namespace: dict[str, Any] = {
            graph_name: graph,
            "pydough": pydough,
        }
        exec(compiled, namespace, namespace)

        loaded_template: Callable = namespace[self.name]
        return loaded_template

    def create_template_def(self) -> str:
        """
        Builds the Python source code for a function definition from this
        template's metadata, without executing or compiling it.

        Uses `self.parameters` to construct the function's signature (typed
        parameters, except those of type "pydough" which are left untyped),
        substitutes positional placeholders (`{1}`, `{2}`, ...) in `self.code`
        with the corresponding parameter names, indents the resulting body,
        and appends a `return` statement for `self.answer_variable`.

        Returns:
            A string containing the full `def <name>(...): ...` source for
            this template, ready to be parsed/executed (e.g. via `from_string`
            or `exec`) elsewhere to obtain the actual callable.
        """
        # Parsing arguments
        arg_names: list[str] = list(self.parameters.keys())
        args_parts: list[str] = []

        part: str
        for key, spec in self.parameters.items():
            part = f"{key}: {spec.type}" if spec.type != "pydough" else f"{key}"
            args_parts.append(part)
        template_args: str = ", ".join(args_parts)

        # --- Replace {1}, {2}, ... with the corresponding argument name ---
        def _replace_placeholder(match: re.Match) -> str:
            index = int(match.group(1))
            if not (1 <= index <= len(arg_names)):
                raise ValueError(
                    f"Placeholder {{{index}}} in pydough_code has no matching "
                    f"argument (only {len(arg_names)} args provided)."
                )
            return arg_names[index - 1]

        substituted_code = re.sub(r"\{(\d+)\}", _replace_placeholder, self.code)

        # --- Indent and assemble the final template ---
        indented_code: str = textwrap.indent(substituted_code.strip(), "    ")

        template_str: str = f"def {self.name}({template_args}):\n{indented_code}\n    return {self.answer_variable}\n"

        return template_str

    def create_template_call(self, kwargs: dict[str, dict[str, str | int]] = {}) -> str:
        """
        Builds the Python source code for a call to this template, without
        executing it.

        Each entry in `kwargs` maps a parameter name to a spec dict with a
        `"value"` (rendered as-is into the call) and, optionally, a `"type"`
        used to decide formatting: values with type `"str"` are wrapped in
        quotes, all others are inserted unquoted (e.g. numeric literals or
        pydough expressions such as `customer.market_segment`).

        Args:
            `kwargs`: a mapping of parameter name to a spec dict, e.g.
            `{"arg_year": {"type": "int", "value": "1996"}}`. Values with
            `"type": "str"` are quoted in the generated call; all other
            values are inserted as raw, unquoted source text.

        Returns:
        A string of the form `"<name>(<param>=<value>, ...)"` representing
        the unevaluated call to this template. The returned string is
        intended to be embedded in a larger source snippet (e.g. via
        `from_string`) rather than executed on its own.
        """

        # TODO: VALIDATE ARG TYPES WITH THE PARAMETERS TYPES
        # NOTE: Add test with date types

        args_parts: list = []
        part: str = ""
        for key, spec in kwargs.items():
            display_quotes: bool = "type" in spec and spec["type"] == "str"

            arg_value = f"'{spec['value']}'" if display_quotes else f"{spec['value']}"
            part = f"{key} = {arg_value}"

            args_parts.append(part)

        args = ", ".join(args_parts)

        return f"""{self.name}({args})"""
