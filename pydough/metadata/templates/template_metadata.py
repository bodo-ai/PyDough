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
    TODO
    """

    name: str
    """
    Name of the parameter
    """

    type: str
    """
    Type of data this parameter receives.
    NOTE: `pydough` type will keep this parameter without type hint.
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
        self._graph: GraphMetadata = graph
        self._name: str = name
        self._code: str = code
        self._answer_variable: str = answer_variable

        self._parameters: dict[str, TemplateParameter] = (
            TemplateMetadata.parse_parameters_from_json(parameters)
        )
        self._template_callable: Callable = self.build_template_callable(graph)

        super().__init__(description, None, None)

    @property
    def graph(self) -> GraphMetadata:
        """
        TODO
        """
        return self._graph

    @property
    def name(self) -> str:
        """
        TODO
        """
        return self._name

    @property
    def parameters(self) -> dict[str, TemplateParameter]:
        """
        TODO
        """
        return self._parameters

    @property
    def code(self) -> str:
        """
        TODO
        """
        return self._code

    @property
    def answer_variable(self) -> str:
        """
        TODO
        """
        return self._answer_variable

    @property
    def template_callable(self) -> Callable | None:
        """
        TODO
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

    def __call__(self, *args, **kwds):
        if self.template_callable is None:
            raise ValueError("")
        return (
            self.template_callable(*args, **kwds)
            if self.template_callable is not None
            else None
        )

    @staticmethod
    def create_error_name(name: str, graph_error_name: str):
        return f"template definition {name!r} in {graph_error_name}"

    @staticmethod
    def parse_from_json(graph: GraphMetadata, name: str, definition_json: dict) -> None:
        """
        TODO
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
        TODO
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

    def build_template_callable(
        self,
        metadata: GraphMetadata | None = None,
    ) -> Callable:
        """
        Builds a callable from a PyDough source string, without executing it.
        Intended for constructing template functions that get stored (e.g. via
        `graph.add_template_definition`) and invoked later.

        Args:
            `template_str`: the PyDough code that forms the body of the function.
            `args`: the parameter names of the generated function.
            `template_name`: the name to give the generated function.
            `answer_variable`: the variable in `source` holding the return value.
            Defaults to "result".
            `metadata`: the metadata graph bound into the function's closure as
            `_graph`. Defaults to `pydough.active_session.metadata`.
            `environment`: extra names available both when transforming the
            source and inside the function's closure.

        Returns:
            The callable built from `source`, not yet invoked.
        """

        template_str: str = self.generate_template_str()

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
            graph_name: metadata,
            "pydough": pydough,
        }
        exec(compiled, namespace, namespace)

        loaded_template: Callable = namespace[self.name]
        return loaded_template

    def generate_template_str(self) -> str:
        """
        TODO
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
