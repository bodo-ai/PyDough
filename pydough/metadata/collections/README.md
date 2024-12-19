# Metadata Collections

This subdirectory of the PyDough metadata directory deals with the definition of PyDough metadata collections.

A collection in PyDough is a group of properties that are logically related and can be accessed together. Collections are used to represent structured data within a graph, and they can contain various types of properties, including inherited properties from other collections.

## Available APIs

The collection metadata has the following notable APIs available for use:

- `name`: a property of the collection metadata storing the name of the collection.
- `graph`: a property that returns the graph to which the collection belongs.
- `properties`: returns a dictionary mapping the names of each property of the collection to the property metadata.
- `inherited_properties`: returns a dictionary mapping the names of each inherited property of the collection to the list of all inherited properties sharing that name.
- `add_property`: a method to add a new property to the collection.
- `add_inherited_property`: a method to add a new inherited property to the collection.
- `get_property_names`: returns a list of the names of all properties in the collection, excluding inherited properties.
- `get_property`: takes in the name of a property in the collection and returns the metadata for that property, or raises an error if there is no property of that name in the collection.

## Hierarchy of Collection Classes

The collection classes in PyDough follow a hierarchy that includes both abstract and concrete classes. Below is a hierarchical list where nesting implies inheritance:

- [`CollectionMetadata`](collection_metadata.py) (abstract): The base abstract class for all collection metadata. It defines the core APIs and properties that all collections must implement.
  - [`SimpleTableMetadata`](simple_table_metadata.py) (concrete): A concrete implementation of `CollectionMetadata` that represents a simple table collection. This class includes additional logic specific to handling simple table structures within a graph.