# Properties Metadata

This subdirectory of the PyDough metadata directory deals with the definition of PyDough properties metadata.

A property in PyDough represents a specific attribute or relationship within a collection. The definition of the various property classes is found in the respective files.

## Available APIs

The properties metadata has the following notable APIs available for use:

- `is_plural`: Indicates if the property can map to multiple values.
- `is_subcollection`: Indicates if the property maps to another collection.
- `is_reversible`: Indicates if the property has a corresponding reverse relationship.
- `verify_json_metadata`: A static method that verifies the JSON describing the metadata for a property. Every concrete class has an implementation of this static method.
- `parse_from_json`: A static method that parses the JSON to create the property and insert it into the collection. Every concrete class has an implementation of this static method.

## Hierarchy of Properties Classes

The properties classes in PyDough follow a hierarchy that includes both abstract and concrete classes. Below is a hierarchical list where nesting implies inheritance:

- `PropertyMetadata` (abstract): Base class for all property metadata. [property_metadata.py](property_metadata.py)
    - `ScalarAttributeMetadata` (abstract): Base class for properties that are scalars within each record of a collection. [scalar_attribute_metadata.py](scalar_attribute_metadata.py)
        - `TableColumnMetadata` (concrete): Represents a column of data from a relational table. [table_column_metadata.py](table_column_metadata.py)
    - `SubcollectionRelationshipMetadata` (abstract): Base class for properties that map to a subcollection of a collection. [subcollection_relationship_metadata.py](subcollection_relationship_metadata.py)
        - `ReversiblePropertyMetadata` (abstract): Base class for properties that map to a subcollection and have a corresponding reverse relationship. [reversible_property_metadata.py](reversible_property_metadata.py)
            - `SimpleJoinMetadata` (concrete): Represents a join between a collection and its subcollection based on equi-join keys. [simple_join_metadata.py](simple_join_metadata.py)
            - `CartesianProductMetadata` (concrete): Represents a cartesian product between a collection and its subcollection. [cartesian_product_metadata.py](cartesian_product_metadata.py)
        - `CompoundRelationshipMetadata` (concrete): Represents a property created by combining two reversible properties that share a middle collection. [compound_relationship_metadata.py](compound_relationship_metadata.py)
    - `InheritedPropertyMetadata` (concrete): Represents a property that is inherited from one of the skipped middle collections in a compound relationship. [inherited_property_metadata.py](inherited_property_metadata.py)