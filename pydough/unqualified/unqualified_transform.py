"""
TODO: add file-level docstring
"""

__all__ = ["init_pydough_context", "transform_code"]

import ast
import inspect
import types


class AddRootVisitor(ast.NodeTransformer):
    """
    AST visitor class that transforms nodes in the following ways:
    1. Whenever a variable is assigned, marks it as a known variable name
    (in addition to any `known_name` values passed in).
    2. Removes the `init_pydough_context` decorator from above any functions.
    3. Adds `ROOT = UnqualifiedRoot(parse_json_from_metadata(file_path, graph_name))`
    to the start of each function body.
    4. Prepends any unknown variable names with `ROOT.`
    """

    def __init__(self, file_path: str, graph_name: str, known_names: set[str]):
        self._file_path: str = file_path
        self._graph_name: str = graph_name
        self._known_names: set[str] = known_names
        self._known_names.update({"UnqualifiedRoot", "parse_json_metadata_from_file"})

    def visit_Assign(self, node):
        for target in node.targets:
            assert isinstance(target, ast.Name)
            self._known_names.add(target.id)
        return self.generic_visit(node)

    def visit_FunctionDef(self, node):
        decorator_list: list[ast.AST] = []
        for deco in node.decorator_list:
            if not (
                isinstance(deco, ast.Call)
                and isinstance(deco.func, ast.Name)
                and deco.func.id == "init_pydough_context"
            ):
                decorator_list.append(deco)
        import_root: ast.AST = ast.ImportFrom(
            module="pydough.unqualified", names=[ast.alias("UnqualifiedRoot")], level=0
        )
        import_parse: ast.AST = ast.ImportFrom(
            module="pydough",
            names=[ast.alias("parse_json_metadata_from_file")],
            level=0,
        )
        root_def: ast.AST = ast.Assign(
            targets=[ast.Name(id="ROOT", ctx=ast.Store())],
            value=ast.Call(
                func=ast.Name(id="UnqualifiedRoot", ctx=ast.Load()),
                args=[
                    ast.Call(
                        func=ast.Name("parse_json_metadata_from_file", ctx=ast.Load()),
                        args=[
                            ast.Constant(value=self._file_path),
                            ast.Constant(value=self._graph_name),
                        ],
                        keywords=[],
                    )
                ],
                keywords=[],
            ),
        )
        result: ast.AST = ast.FunctionDef(
            name=node.name,
            args=node.args,
            body=[import_root, import_parse, root_def] + node.body,
            decorator_list=node.decorator_list,
            type_params=node.type_params,
            returns=node.returns,
        )
        answer: ast.AST = self.generic_visit(result)
        return answer

    def visit_Name(self, node):
        unrecognized_var: bool = False
        if node.id not in self._known_names:
            try:
                eval(node.id)
            except NameError:
                unrecognized_var = True
        if unrecognized_var:
            result = ast.Attribute(
                value=ast.Name(id="ROOT", ctx=ast.Load()), attr=node.id, ctx=node.ctx
            )
            return result
        else:
            return node


def transform_code(
    source: str, file_path: str, graph_name: str, known_names: set[str]
) -> ast.AST:
    """
    Transforms the source code into a new Python AST that has had the PyDough
    decorator removed, had the definition of `ROOT` injected at the top of the
    function body, and prepend unknown variables with `ROOT.`

    Args:
        `source`: the raw Python code string for the original function.
        `file_path`: the path to the JSON file containing the metadata for
        the PyDough graph that should be used.
        `graph_name`: the name of the graph from the JSON file that should be
        used.
        `known_names`: the set of strings representing names of variables that
        are known to be accessible by the function that are not defined within,
        such as global variables or module imports.

    Returns:
        The Python AST for the transformed code.
    """
    visitor: ast.NodeTransformer = AddRootVisitor(file_path, graph_name, known_names)
    source = source.lstrip("\n")
    n_strip = len(source) - len(source.lstrip())
    if n_strip > 0:
        source = "\n".join(line[n_strip:] for line in source.splitlines())
    tree = ast.parse(source)
    assert isinstance(tree, ast.AST)
    new_tree = ast.fix_missing_locations(visitor.visit(tree))
    assert isinstance(new_tree, ast.AST)
    return new_tree


def init_pydough_context(file_path: str, graph_name: str):
    """
    Decorator that wraps around a PyDough function and transforms its body into
    UnqualifiedNodes by prepending unknown variables with `ROOT.`

    Args:
        `file_path`: the path to the JSON file containing the metadata for
        the PyDough graph that should be used.
        `graph_name`: the name of the graph from the JSON file that should be
        used.
    """

    def decorator(func):
        source: str = inspect.getsource(func)
        new_tree: ast.AST = transform_code(
            source, file_path, graph_name, set(func.__globals__)
        )
        assert isinstance(new_tree, ast.Module)
        file_name: str = func.__code__.co_filename
        new_code = compile(new_tree, file_name, "exec")
        idx = -1
        for i in range(len(new_code.co_consts)):
            if new_code.co_consts[i].__class__.__name__ == "code":
                idx = i
                break
        assert idx >= 0, "Did not find a code object in the compiled code"
        new_func = types.FunctionType(new_code.co_consts[idx], func.__globals__)
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
