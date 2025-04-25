"""
Definition of PyDough metadata for a property that connects two collections by
combining two other subcollection properties that share a middle collection.
"""

__all__ = ["CompoundRelationshipMetadata"]


from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.errors import (
    HasPropertyWith,
    HasType,
    NoExtraKeys,
    NonEmptyListOf,
    PossiblyEmptyMapOf,
    PyDoughMetadataException,
    compound_relationship_inherited_predicate,
    is_bool,
    is_string,
)

from .property_metadata import PropertyMetadata
from .reversible_property_metadata import ReversiblePropertyMetadata


class CompoundRelationshipMetadata(ReversiblePropertyMetadata):
    """
    Concrete metadata implementation for a PyDough property created by
    combining two reversible properties, one mapping a collection to
    one of its sub-collections and the other mapping that subcollection
    to one of its sub-collections. A property also grants access to
    certain inherited properties derived from the middle collection.
    """

    # Set of names of fields that can be included in the JSON object
    # describing a compound relationship property.
    allowed_fields: set[str] = PropertyMetadata.allowed_fields | {
        "primary_property",
        "secondary_property",
        "reverse_relationship_name",
        "singular",
        "no_collisions",
        "inherited_properties",
    }

    def __init__(
        self,
        name: str,
        reverse_name: str,
        collection: CollectionMetadata,
        other_collection: CollectionMetadata,
        singular: bool,
        no_collisions: bool,
        primary_property: ReversiblePropertyMetadata,
        secondary_property: ReversiblePropertyMetadata,
        inherited_properties: dict[str, PropertyMetadata],
    ):
        from .inherited_property_metadata import InheritedPropertyMetadata

        super().__init__(
            name, reverse_name, collection, other_collection, singular, no_collisions
        )
        HasType(ReversiblePropertyMetadata).verify(primary_property, self.error_name)
        HasType(ReversiblePropertyMetadata).verify(secondary_property, self.error_name)
        PossiblyEmptyMapOf(is_string, HasType(PropertyMetadata)).verify(
            inherited_properties,
            f"property 'inherited_properties' of {self.error_name}",
        )
        self._primary_property: ReversiblePropertyMetadata = primary_property
        self._secondary_property: ReversiblePropertyMetadata = secondary_property

        # Properties that are passed in as inherited properties via the metadata
        self._inherited_properties: dict[str, PropertyMetadata] = {}
        for alias, property in inherited_properties.items():
            self._inherited_properties[alias] = InheritedPropertyMetadata(
                alias, other_collection, self, property
            )

    @property
    def primary_property(self) -> ReversiblePropertyMetadata:
        """
        The property used to map the collection to the middle collection.
        """
        return self._primary_property

    @property
    def secondary_property(self) -> ReversiblePropertyMetadata:
        """
        The property used to map the middle collection to the other collection.
        """
        return self._secondary_property

    @property
    def inherited_properties(self) -> dict[str, PropertyMetadata]:
        """
        The properties inherited by using this compound relationship,
        represented as a mapping of an alias name to the actual property.
        """
        return self._inherited_properties

    @staticmethod
    def create_error_name(name: str, collection_error_name: str):
        return f"compound property {name!r} of {collection_error_name}"

    @property
    def components(self) -> list:
        inherited_properties_dict = {
            alias: property.name
            for alias, property in self.inherited_properties.items()
        }
        comp: list = super().components
        comp.append(self.primary_property.name)
        comp.append(self.secondary_property.name)
        comp.append(inherited_properties_dict)
        return comp

    @staticmethod
    def verify_json_metadata(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        Verifies that the JSON describing the metadata for a property within
        a collection is well-formed to create a new
        CompoundRelationshipMetadata instance. Should be dispatched from
        PropertyMetadata.verify_json_metadata which implements more generic
        checks.

        Args:
            `collection`: the metadata for the PyDough collection that the
            property would be inserted into.
            `property_name`: the name of the property that would be inserted.
            `property_json`: the JSON object that would be parsed to create
            the new property.

        Raises:
            `PyDoughMetadataException`: if the JSON for the property is
            malformed.
        """
        # Create the string used to identify the property in error messages.
        error_name = f"compound relationship property {property_name!r} of {collection.error_name}"

        # Verify that the JSON has the required `primary_property`,
        # `secondary_property`, `reverse_relationship_name`, `singular`,
        # `no_collisions` and `inherited_properties` fields, without anything
        # extra.
        HasPropertyWith("primary_property", is_string).verify(property_json, error_name)
        HasPropertyWith("secondary_property", is_string).verify(
            property_json, error_name
        )
        HasPropertyWith("reverse_relationship_name", is_string).verify(
            property_json, error_name
        )
        HasPropertyWith("singular", is_bool).verify(property_json, error_name)
        HasPropertyWith("no_collisions", is_bool).verify(property_json, error_name)
        HasPropertyWith(
            "inherited_properties", compound_relationship_inherited_predicate
        ).verify(property_json, error_name)
        NoExtraKeys(CompoundRelationshipMetadata.allowed_fields).verify(
            property_json, error_name
        )

    @staticmethod
    def parse_from_json(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        Procedure dispatched from PropertyMetadata.parse_from_json to handle
        the parsing for compound relationship properties.

        Args:
            `collection`: the metadata for the PyDough collection that the
            property would be inserted nto.
            `property_name`: the name of the property that would be inserted.
            `property_json`: the JSON object that would be parsed to create
            the new table column property.

        Raises:
            `PyDoughMetadataException`: if the JSON for the property is
            malformed.
        """
        from .inherited_property_metadata import InheritedPropertyMetadata

        # Extract the name of the primary/secondary properties, the inherited
        # properties mapping, the reverse relationship name, and the singular /
        # no_collisions fields from the JSON.
        primary_property_name: str = property_json["primary_property"]
        secondary_property_name: str = property_json["secondary_property"]
        inherited_properties_mapping: dict[str, str] = property_json[
            "inherited_properties"
        ]
        singular: bool = property_json["singular"]
        no_collisions: bool = property_json["no_collisions"]
        reverse_name: str = property_json["reverse_relationship_name"]

        # Extract the primary property from the current collection's
        # properties. Assumes that the primary property has already been
        # defined and added to the collection's properties.
        HasPropertyWith(
            primary_property_name, HasType(ReversiblePropertyMetadata)
        ).verify(collection.properties, collection.error_name)
        primary_property: ReversiblePropertyMetadata = collection.properties[
            primary_property_name
        ]
        secondary_collection: CollectionMetadata = primary_property.other_collection

        # Extract the secondary property from the middle collection's
        # properties. Assumes that the secondary property has already been
        # defined and added to the middle collection's properties.
        HasPropertyWith(
            secondary_property_name, HasType(ReversiblePropertyMetadata)
        ).verify(secondary_collection.properties, secondary_collection.error_name)
        secondary_property: ReversiblePropertyMetadata = (
            secondary_collection.properties[secondary_property_name]
        )
        other_collection: CollectionMetadata = secondary_property.other_collection

        # Obtain the inherited properties by mapping each of the alias names in
        # the JSON to the corresponding property metadata from the desired
        # property of the middle collection. Assumes that all the inherited
        # properties have already been defined added to the middle collection's
        # properties.
        inherited_properties: dict[str, PropertyMetadata] = {}
        for alias_name, inherited_property_name in inherited_properties_mapping.items():
            has_property: bool = HasPropertyWith(
                inherited_property_name, HasType(PropertyMetadata)
            ).accept(secondary_collection.properties)
            has_inherited_property: bool = HasPropertyWith(
                inherited_property_name,
                NonEmptyListOf(HasType(InheritedPropertyMetadata)),
            ).accept(secondary_collection.inherited_properties)
            failure_msg: str = f"Cannot find property to inherit {inherited_property_name!r} in compound relationship {property_name} of {collection.error_name}"
            ambiguous_message: str = (
                f"{failure_msg} due to ambiguous inherited properties."
            )
            if has_property:
                # The simple case where the property is a direct property of
                # the middle collection.
                inherited_property: PropertyMetadata = secondary_collection.properties[
                    inherited_property_name
                ]
                inherited_properties[alias_name] = inherited_property
            elif has_inherited_property:
                # The more complex case where the property is an inherited property
                # of the middle collection, either from the primary or secondary
                # property. In this case, all candidate inherited properties must be
                # searched to verify that one of them comes from the primary or secondary
                # property, and there are not multiple successful candidates.
                candidates: list[InheritedPropertyMetadata] = (
                    secondary_collection.inherited_properties[inherited_property_name]
                )
                primary_candidate = None
                secondary_candidate = None
                for candidate_inherited_property in candidates:
                    original: ReversiblePropertyMetadata = (
                        candidate_inherited_property.property_inherited_from
                    )
                    reverse: ReversiblePropertyMetadata = original.reverse_property
                    if primary_property in (original, reverse):
                        if (
                            primary_candidate is not None
                            or secondary_candidate is not None
                        ):
                            raise PyDoughMetadataException(ambiguous_message)
                        if primary_property == reverse:
                            candidate_inherited_property = (
                                candidate_inherited_property.flip_source()
                            )
                        primary_candidate = candidate_inherited_property
                    if secondary_property in (original, reverse):
                        if (
                            primary_candidate is not None
                            or secondary_candidate is not None
                        ):
                            raise PyDoughMetadataException(ambiguous_message)
                        if secondary_property == reverse:
                            candidate_inherited_property = (
                                candidate_inherited_property.flip_source()
                            )
                        secondary_candidate = candidate_inherited_property
                if primary_candidate is not None:
                    inherited_properties[alias_name] = primary_candidate
                elif secondary_candidate is not None:
                    inherited_properties[alias_name] = secondary_candidate
                else:
                    raise PyDoughMetadataException(failure_msg)
            else:
                raise PyDoughMetadataException(failure_msg)

        # Build the new property, its reverse, then add both
        # to their collection's properties.
        property = CompoundRelationshipMetadata(
            property_name,
            reverse_name,
            collection,
            other_collection,
            singular,
            no_collisions,
            primary_property,
            secondary_property,
            inherited_properties,
        )
        property.build_reverse_relationship()
        collection.add_property(property)
        other_collection.add_property(property.reverse_property)

        # Add the inherited properties from the compound relationship
        # and its reverse to the collection & subcollection it maps to.
        for inherited_property in property.inherited_properties.values():
            other_collection.add_inherited_property(inherited_property)
        assert isinstance(property.reverse_property, CompoundRelationshipMetadata)
        for (
            inherited_property
        ) in property.reverse_property.inherited_properties.values():
            collection.add_inherited_property(inherited_property)

    def build_reverse_relationship(self) -> None:
        from .inherited_property_metadata import InheritedPropertyMetadata

        # Flip the sources of the inherited properties that are also inherited properties
        new_properties_to_inherit: dict[str, PropertyMetadata] = {}
        for alias, property in self._inherited_properties.items():
            assert isinstance(property, InheritedPropertyMetadata)
            if isinstance(property.property_to_inherit, InheritedPropertyMetadata):
                new_properties_to_inherit[alias] = (
                    property.property_to_inherit.flip_source()
                )
            else:
                new_properties_to_inherit[alias] = property.property_to_inherit
        # Construct the reverse relationship by flipping the forward & reverse
        # names, the source / target collections, and the plural properties.
        # The new primary property is the reverse of the secondary property.
        # The new secondary property is the reverse of the primary property.
        reverse = CompoundRelationshipMetadata(
            self.reverse_name,
            self.name,
            self.other_collection,
            self.collection,
            self.no_collisions,
            self.singular,
            self.secondary_property.reverse_property,
            self.primary_property.reverse_property,
            new_properties_to_inherit,
        )

        # Then fill the `reverse_property` fields with one another.
        reverse._reverse_property = self
        self._reverse_property = reverse
