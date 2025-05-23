# Properties Metadata

This subdirectory of the PyDough metadata directory deals with the definition of PyDough properties metadata.

A property in PyDough represents a specific attribute or relationship within a collection. The definition of the various property classes is found in the respective files.

## Available APIs

The properties metadata has the following notable APIs available for use:

- `is_plural`: Indicates if the property can map to multiple values.
- `is_subcollection`: Indicates if the property maps to another collection.
- `is_reversible`: Indicates if the property has a corresponding reverse relationship.
- `parse_from_json`: A static method that parses the JSON to create the property and insert it into the collection. Every concrete class has an implementation of this static method.

## Hierarchy of Properties Classes

The properties classes in PyDough follow a hierarchy that includes both abstract and concrete classes. Below is a hierarchical list where nesting implies inheritance:

- [`PropertyMetadata`](property_metadata.py) (abstract): Base class for all property metadata.
    - [`ScalarAttributeMetadata`](scalar_attribute_metadata.py) (abstract): Base class for properties that are scalars within each record of a collection.
        - [`TableColumnMetadata`](table_column_metadata.py) (concrete): Represents a column of data from a relational table.
    - [`SubcollectionRelationshipMetadata`](subcollection_relationship_metadata.py) (abstract): Base class for properties that map to a subcollection of a collection.
        - [`ReversiblePropertyMetadata`](reversible_property_metadata.py) (abstract): Base class for properties that map to a subcollection and have a corresponding reverse relationship.
            - [`CartesianProductMetadata`](cartesian_product_metadata.py) (concrete): Represents a cartesian product between a collection and its subcollection.
            - [`SimpleJoinMetadata`](simple_join_metadata.py) (concrete): Represents a join between a collection and its subcollection based on equi-join keys.
            - ['GeneralJoinMetadata](general_join_metadata.py) (concrete): Represents a join between a collection and its subcollection based on an arbitrary join condition, as opposed to equi-join keys.