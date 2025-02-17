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
    by adding it to the current scope set (in addition to all the previous scopes).
    New scopes are created for functions, lambda functions, comprehensions,
    context managers, etc.
    2. Removes the `init_pydough_context` decorator from above any functions.
    3. Adds `_ROOT = UnqualifiedRoot(graph)` to the start of each function body.
    4. Prepends any unknown variable names with `_ROOT.`
    """

    def __init__(self, graph_name: str, known_names: set[str]):
        self._graph_name = graph_name
        # Initialize the scope stack with the outermost scope, including `known_names`
        # set which would contain global variables or module imports of the
        # jupyter cell or function.
        self._scope_stack: list[set[str]] = [
            set({"UnqualifiedRoot", self._graph_name, *known_names})
        ]

    def current_scope(self) -> set[str]:
        # Return the current scope
        return self._scope_stack[-1]

    def enter_scope(self) -> None:
        # Inherit parent scope but creates new copy
        self._scope_stack.append(set(self.current_scope()))

    def exit_scope(self) -> None:
        self._scope_stack.pop()

    def visit_Module(self, node) -> ast.AST:
        """
        Visit the root node.
        """
        # Create the root definition in the outermost body
        node.body = self.create_root_def() + node.body
        return self.generic_visit(node)

    def visit_Assign(self, node) -> ast.AST:
        """
        Handle unpacking assignments like `a, (b, c) = ...`
        """
        for target in node.targets:
            self._scope_targets(target)  # Reuse existing scope-tracking logic
        return self.generic_visit(node)

    def visit_AnnAssign(self, node) -> ast.AST:
        """
        Handle annotated assignments like `a: int = ...`
        """
        self._scope_targets(node.target)
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

    def visit_FunctionDef(self, node) -> ast.AST:
        """
        Tracks function parameters in the scope and removes the PyDough decorator.

        Example:
            def interval_n(n, name="test"):
                return COUNT(customers.WHERE(
                    MONOTONIC(n * 1000, acctbal,  (n + 1) * 1000)))
        """
        self.current_scope().add(node.name)
        self.enter_scope()
        params = []
        params += [p.arg for p in node.args.posonlyargs]
        params += [p.arg for p in node.args.args]
        params += [p.arg for p in node.args.kwonlyargs]
        if node.args.vararg:
            params.append(node.args.vararg.arg)
        if node.args.kwarg:
            params.append(node.args.kwarg.arg)
        self.current_scope().update(params)
        decorator_list: list[ast.expr] = []
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
                decorator_list=decorator_list,
                type_params=node.type_params,
                returns=node.returns,
            )
        else:
            result = ast.FunctionDef(  # type: ignore
                name=node.name,
                args=node.args,
                body=prefix + node.body,
                decorator_list=decorator_list,
                returns=node.returns,
            )
        answer: ast.AST = self.generic_visit(result)
        self.exit_scope()
        return answer

    def visit_expression(self, node) -> ast.expr:
        result = self.generic_visit(node)
        assert isinstance(result, ast.expr)
        return result

    def visit_statement(self, node) -> ast.stmt:
        result = self.generic_visit(node)
        assert isinstance(result, ast.stmt)
        return result

    def visit_For(self, node) -> ast.AST:
        self._scope_targets(node.target)
        # Visit the rest of the node as usual
        return self.generic_visit(node)

    def visit_Name(self, node) -> ast.AST:
        unrecognized_var: bool = False
        if not any(node.id in scope for scope in self._scope_stack):
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

    def visit_Lambda(self, node: ast.Lambda) -> ast.AST:
        """
        Handle lambda function parameters and scoping.

        Example:
            interval_n = lambda n: COUNT(
                customers.WHERE(MONOTONIC(n * 1000, acctbal, (n + 1) * 1000))
            )
        """
        # Enter new scope
        self.enter_scope()

        # Extract and track lambda parameters
        params: list[str] = []
        params += [p.arg for p in node.args.posonlyargs]
        params += [p.arg for p in node.args.args]
        params += [p.arg for p in node.args.kwonlyargs]
        if node.args.vararg:
            params.append(node.args.vararg.arg)
        if node.args.kwarg:
            params.append(node.args.kwarg.arg)
        self.current_scope().update(params)

        # Visit the lambda body (an expression)
        answer: ast.AST = self.generic_visit(node)

        # Exit scope
        self.exit_scope()

        return answer

    def visit_DictComp(self, node: ast.DictComp) -> ast.AST:
        """
        Handle dictionary comprehensions.

        Example:
            {k: v for k in range(3) for v in range(3)}
        """
        # New scope for comprehension
        self.enter_scope()

        # Track generator targets
        # Example: generator: "for k, v in items"; targets being "k,v"
        # Note that there can be multiple generators
        # (e.g. "{k: v for k in range(3) for v in range(3)}")
        # This gives us node.generators => [for k in range(3),for v in range(3)]
        for generator in node.generators:
            self._scope_targets(generator.target)

        # Transform key and value. We use self.visit as opposed to self.generic_visit as
        # we would like to dispatch to a specific visitor method such as visit_Name, etc.
        new_key: ast.expr = self.visit(node.key)
        new_value: ast.expr = self.visit(node.value)
        answer: ast.AST = ast.DictComp(
            key=new_key,
            value=new_value,
            generators=[self.visit(gen) for gen in node.generators],
        )
        self.exit_scope()
        return answer

    def visit_ListComp(self, node: ast.ListComp) -> ast.AST:
        """
        Handle list comprehensions.

        Example:
            [ord(c) for line in file for c in line]
        """
        self.enter_scope()  # New scope for comprehension
        # Track generator targets (e.g., i in "for i in items")
        for generator in node.generators:
            self._scope_targets(generator.target)
        # Transform elt. The elt attribute is an ast node that corresponds
        # to the expression before the first 'for' in a list comprehension.
        new_elt: ast.expr = self.visit(node.elt)
        answer: ast.expr = ast.ListComp(
            elt=new_elt,
            generators=[self.visit(gen) for gen in node.generators],
        )
        self.exit_scope()
        return answer

    def visit_SetComp(self, node: ast.SetComp) -> ast.AST:
        """
        Handle set comprehensions.

        Example:
            {
                COUNT(customers.WHERE(MONOTONIC(i * 1000, acctbal, (i + 1) * 1000)))
                for i in range(3)
            }
        """
        self.enter_scope()  # New scope for comprehension
        # Track generator targets (e.g., i in "for i in items")
        for generator in node.generators:
            self._scope_targets(generator.target)
        # Transform elt. The elt attribute is an ast node that corresponds
        # to the expression before the first for in a set comprehension.
        new_elt: ast.expr = self.visit(node.elt)
        answer: ast.expr = ast.SetComp(
            elt=new_elt,
            generators=[self.visit(gen) for gen in node.generators],
        )
        self.exit_scope()
        return answer

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> ast.AST:
        """
        Handle generator comprehensions.

        Example:
            (COUNT(customers.WHERE(MONOTONIC(i * 1000, acctbal, (i + 1) * 1000)))
                for i in range(3))
        """
        self.enter_scope()  # New scope for comprehension
        # Track generator targets (e.g., i in "for i in items")
        for generator in node.generators:
            self._scope_targets(generator.target)
        # Transform elt. The elt attribute is an ast node that corresponds
        # to the expression before the first for in a generator comprehension.
        new_elt: ast.expr = self.visit(node.elt)
        answer: ast.expr = ast.GeneratorExp(
            elt=new_elt,
            generators=[self.visit(gen) for gen in node.generators],
        )
        self.exit_scope()
        return answer

    def _scope_targets(self, target: ast.expr) -> None:
        """
        Add variables to the current scope.
        """
        # if target is a tuple like (k,v).
        if isinstance(target, (ast.Tuple, ast.List)):
            for elt in target.elts:
                if isinstance(elt, ast.Name):
                    self.current_scope().add(elt.id)
                elif isinstance(elt, (ast.Tuple, ast.List)):
                    self._scope_targets(elt)
        # if target is single variable like k.
        elif isinstance(target, ast.Name):
            self.current_scope().add(target.id)

    def visit_With(self, node: ast.With) -> ast.AST:
        """
        Handle context manager variables declared with `as`
        """
        # Track all variables bound by context manager(s)
        self.enter_scope()
        # Let's loop through the contexts. (e.g tf.TemporaryFile() as tf_handle1)
        for item in node.items:
            if item.optional_vars:
                self._scope_targets(item.optional_vars)
        answer = self.generic_visit(node)
        self.exit_scope()
        return answer

    def visit_Import(self, node: ast.Import) -> ast.AST:
        """
        Track imported module aliases like `import x as y`, `import a.b.c`
        """
        for alias in node.names:
            name = alias.asname or alias.name.split(".", 1)[0]
            self.current_scope().add(name)
        return self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> ast.AST:
        """
        Track specific imports like `from x import y as z`
        """
        for alias in node.names:
            name = alias.asname or alias.name
            self.current_scope().add(name)
        return self.generic_visit(node)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> ast.AST:
        """
        Track exception variables like `except Error as err`
        """
        # Add exception binding name to current scope
        if node.name:
            self.current_scope().add(node.name)
        # Process body normally
        return self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> ast.AST:
        """
        Handle class definitions and their scoped content
        """
        # Track class name in current scope
        self.current_scope().add(node.name)
        return self.generic_visit(node)


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
        # Only get the CodeType corresponding to the decorated function
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
