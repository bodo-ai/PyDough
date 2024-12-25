# Import the following into your test file (I strongly advice against ever using import *):
import pydough
from pydough import init_pydough_context
from pydough.unqualified import qualify_node
from pydough.conversion import convert_ast_to_relational
from sqlglot.dialects.sqlite import SQLite as SQLiteDialect
from pydough.sqlglot import SQLGlotRelationalVisitor
from pydough.sqlglot import execute

# Write a function that takes in no arguments and returns your PyDough answer
# (printing hasn't been implemented yet). # See tpch_test_functions.py for more examples.
# You need to put this decorator on top of this function so PyDough does its magic, and
# replace `file_path` and `graph_name` with the path to your JSON file & the name of the
# graph you are using.
@init_pydough_context("test_metadata/sample_graphs.json", "TPCH")
def pydough_func_16():
 return TPCH(nn=COUNT(Regions)).Nations(name, BACK(1).nn, n_suppliers=COUNT(suppliers), r_name = region.name)

# This is the test function. You can add whatever prints/asserts you want at any stage
def test_pydough_func_16(): # <-- `sqlite_tpch_db_context` is a fixture
 # Get the PyDough qualified AST structure. Can print `qualified_node.to_tree_string()` to
 # see its structure


 graph = pydough.parse_json_metadata_from_file(
            file_path="test_metadata/sample_graphs.json", graph_name="TPCH")

 unqualified_node = pydough_func_16()
 qualified_node = qualify_node(unqualified_node, graph)

 print(unqualified_node)
 print(qualified_node.to_tree_string())
 print(qualified_node.to_string())

 
test_pydough_func_16()


