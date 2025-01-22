"""
Logic for transforming raw Python code into PyDough code by replacing undefined
variables with unqualified nodes by prepending with with `_ROOT.`.
"""

__all__ = ["init_pydough_context", "transform_cell", "transform_code"]

import ast
import inspect
import types

from pydough.metadata import GraphMetadata


class AddRootVisitor(ast.NodeTransformer):
    """
    QDAG visitor class that transforms nodes in the following ways:
    1. Whenever a variable is assigned, marks it as a known variable name
    (in addition to any `known_name` values passed in).
    2. Removes the `init_pydough_context` decorator from above any functions.
    3. Adds `_ROOT = UnqualifiedRoot(graph)` to the start of each function body.
    4. Prepends any unknown variable names with `_ROOT.`
    """

    def __init__(self, graph_name: str, known_names: set[str]):
        self._graph_name = graph_name
        self._known_names: set[str] = known_names
        self._known_names.update({"UnqualifiedRoot", self._graph_name})

    def visit_Module(self, node):
        """Visit the root node."""
        # Create the root definition in the outermost body
        node.body = self.create_root_def() + node.body
        return self.generic_visit(node)

    def visit_Assign(self, node):
        for target in node.targets:
            if isinstance(target, ast.Name):
                self._known_names.add(target.id)
        return self.generic_visit(node)

    def create_root_def(self) -> list[ast.AST]:
        import_root: ast.AST = ast.ImportFrom(
            module="pydough.unqualified", names=[ast.alias("UnqualifiedRoot")], level=0
        )
        root_def: ast.AST = ast.Assign(
            targets=[ast.Name(id="_ROOT", ctx=ast.Store())],
            value=ast.Call(
                func=ast.Name(id="UnqualifiedRoot", ctx=ast.Load()),
                args=[ast.Name(id=self._graph_name, ctx=ast.Load())],
                keywords=[],
            ),
        )
        return [import_root, root_def]

    def visit_FunctionDef(self, node):
        decorator_list: list[ast.AST] = []
        for deco in node.decorator_list:
            if not (
                isinstance(deco, ast.Call)
                and isinstance(deco.func, ast.Name)
                and deco.func.id == "init_pydough_context"
            ):
                decorator_list.append(deco)
        prefix: list[ast.AST] = self.create_root_def()
        result: ast.AST
        if hasattr(node, "type_params"):
            result = ast.FunctionDef(  # type: ignore
                name=node.name,
                args=node.args,
                body=prefix + node.body,
                decorator_list=node.decorator_list,
                type_params=node.type_params,
                returns=node.returns,
            )
        else:
            result = ast.FunctionDef(  # type: ignore
                name=node.name,
                args=node.args,
                body=prefix + node.body,
                decorator_list=node.decorator_list,
                returns=node.returns,
            )
        answer: ast.AST = self.generic_visit(result)
        return answer

    def visit_expression(self, node) -> ast.expr:
        result = self.generic_visit(node)
        assert isinstance(result, ast.expr)
        return result

    def visit_statement(self, node) -> ast.stmt:
        result = self.generic_visit(node)
        assert isinstance(result, ast.stmt)
        return result

    def visit_For(self, node):
        if isinstance(node.target, ast.Name):
            self._known_names.add(node.target.id)
        return ast.For(  # type: ignore
            target=node.target,
            iter=self.visit_expression(node.iter),
            body=[self.visit_statement(elem) for elem in node.body],
            orelse=[self.visit_statement(elem) for elem in node.orelse],
            type_comment=node.type_comment,
        )

    def visit_Name(self, node):
        unrecognized_var: bool = False
        if node.id not in self._known_names:
            try:
                eval(node.id)
            except NameError:
                unrecognized_var = True
        if unrecognized_var:
            result = ast.Attribute(
                value=ast.Name(id="_ROOT", ctx=ast.Load()), attr=node.id, ctx=node.ctx
            )
            return result
        else:
            return node


def transform_code(
    source: str, graph_dict: dict[str, GraphMetadata], known_names: set[str]
) -> ast.AST:
    """
    Transforms the source code into a new Python QDAG that has had the PyDough
    decorator removed, had the definition of `_ROOT` injected at the top of the
    function body, and prepend unknown variables with `_ROOT.`

    Args:
        `source`: the raw Python code string for the original function.
        `graph_dict`: a dictionary mapping the name of the graph to the
            metadata graph.
        `known_names`: the set of strings representing names of variables that
        are known to be accessible by the function that are not defined within,
        such as global variables or module imports.

    Returns:
        The Python QDAG for the transformed code.
    """
    assert len(graph_dict) == 1, "Expected exactly one key in the graph_dict"
    visitor: ast.NodeTransformer = AddRootVisitor(
        list(graph_dict.keys())[0], known_names
    )
    source = source.lstrip("\n")
    n_strip = len(source) - len(source.lstrip())
    if n_strip > 0:
        source = "\n".join(line[n_strip:] for line in source.splitlines())
    tree = ast.parse(source)
    assert isinstance(tree, ast.AST)
    new_tree = ast.fix_missing_locations(visitor.visit(tree))
    assert isinstance(new_tree, ast.AST)
    return new_tree


def transform_cell(cell: str, graph_name: str, known_names: set[str]) -> str:
    """
    Transforms the source code from Juypter into an updated version with
    resolved names.

    Args:
        `source`: the raw Python code string for the original function.
        `graph_name`: The name of the graph to use as a variable.
        `known_names`: the set of strings representing names of variables that
        are known to be accessible by the function that are not defined within,
        such as global variables or module imports.

    Returns:
        The updated unparsed source code.
    """
    visitor: ast.NodeTransformer = AddRootVisitor(graph_name, known_names)
    tree = ast.parse(cell)
    assert isinstance(tree, ast.AST)
    new_tree = ast.fix_missing_locations(visitor.visit(tree))
    assert isinstance(new_tree, ast.AST)
    return ast.unparse(new_tree)


def init_pydough_context(graph: GraphMetadata):
    """
    Decorator that wraps around a PyDough function and transforms its body into
    UnqualifiedNodes by prepending unknown variables with `_ROOT.`

    Args:
        `graph`: The metadata graph to use.
    """

    def decorator(func):
        source: str = inspect.getsource(func)
        graph_dict: dict[str, GraphMetadata] = {"_graph_value": graph}
        new_tree: ast.AST = transform_code(source, graph_dict, set(func.__globals__))
        assert isinstance(new_tree, ast.Module)
        file_name: str = func.__code__.co_filename
        new_code = compile(new_tree, file_name, "exec")
        idx = -1
        for i in range(len(new_code.co_consts)):
            if new_code.co_consts[i].__class__.__name__ == "code":
                idx = i
                break
        assert idx >= 0, "Did not find a code object in the compiled code"
        new_func = types.FunctionType(
            new_code.co_consts[idx], func.__globals__ | graph_dict
        )
        #######################################################################
        ###              FOR DEBUGGING: UNCOMMENT THIS SECTION              ###
        #######################################################################
        # try:
        #     new_func()
        # except Exception as e:
        #     import traceback

        #     print(ast.unparse(new_tree))
        #     print(e)
        #     print(traceback.format_exc())
        return new_func

    return decorator
