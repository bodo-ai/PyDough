# PyDough DSL Spec

This page describes the specification of the PyDough DSL. The specification includes rules of how PyDough code should be structured and the semantics that are used when evaluating PyDough code. Not every feature in the spec is implemented in PyDough as of this time.

## Example Graph

The examples in this document use a metadata graph (named `GRAPH`) with the following collections:
- `People`: records of every known person. Scalar properties: `first_name`, `middle_name`, `last_name`, `ssn`, `birth_date`, `email`, `current_address_id`.
- `Addresses`: records of every known address. Scalar properties: `address_id`, `street_number`, `street_name`, `apartment`, `zip_code`, `city`, `state`.
- `Packages`: records of every known package. Scalar properties: `package_id`, `customer_ssn`, `shipping_address_id`, `billing_address_id`, `order_date`, `arrival_date`.

There are also the following sub-collection relationships:
- `People.packages`: every package ordered by each person (reverse is `Packages.customer`). There can be 0, 1 or multiple packages ordered by a single person, but each package has exactly one person who ordered it.
- `People.current_address`: the current address of each person, if one exists (reverse is `Addresses.current_occupants`). Each person has at most 1 current address (which can be missing), but each address can have 0, 1, or multiple people currently occupying it.
- `Packages.shipping_address`: the address that the package is shipped to (reverse is `Addresses.packages_shipped`). Every package has exactly one shipping address, but each address can have 0, 1 or multiple packages shipped to it.
- `Packages.billing_address`: the address that the package is billed to (reverse is `Addresses.packages_billed`).  Every package has exactly one billing address, but each address can have 0, 1 or multiple packages billed to it.

## Collections

The simplest PyDough code is scanning an entire collection. This is done by providing the name of the collection in the metadata. However, if that name is already used as a variable, then PyDough will not know to replace the name with the corresponding PyDough object.

Good Example #1: obtains every record of the `People` collection. Every scalar property of `People` (`first_name`, `middle_name`, `last_name`, `ssn`, `birth_date`, `email`, `current_address_id`) is automatically included in the output.

```py
%%pydough
People
```

Good Example #2: obtains every record of the `Addresses` collection. The `GRAPH.` prefix is optional and implied when the term is a collection name in the graph. Every scalar property of `Addresses` (`address_id`, `street_number`, `street_name`, `apartment`, `zip_code`, `city`, `state`) is automatically included in the output.

```py
%%pydough
GRAPH.Addresses
```

Bad Example #1: obtains every record of the `Products` collection (there is no `Products` collection).

```py
%%pydough
Addresses
```

Bad Example #2: obtains every record of the `Addresses` collection (but the name `Addresses` has been reassigned to a variable).

```py
%%pydough
Addresses = 42
Addresses
```

Bad Example #3: obtains every record of the `Addresses` collection (but the graph name `HELLO` is the wrong graph name for this example).

```py
%%pydough
HELLO.Addresses
```

### Sub-Collections

The next step in PyDough after accessing a collection is accessing any of its sub-collections. The syntax `collection.subcollection` steps into every record of `subcollection` for each record of `collection`. This can result in changes of cardinality if records of `collection` can have multiple records of `subcollection`, and can result in duplicate records in the output if records of `subcollection` can be sourced from different records of `collection`.

Good Example #1: for every person, obtains their current address. Every scalar property of `Addresses` (`address_id`, `street_number`, `street_name`, `apartment`, `zip_code`, `city`, `state`) is automatically included in the output. A record from `Addresses` can be included multiple times if multiple different `People` records have it as their current address, or it could be missing entirely if no person has it as their current address.

```py
%%pydough
People.current_addresses
```

Good Example #2: for every package, obtains the person who shipped it address. The `GRAPH.` prefix is optional and implied when the term is a collection name in the graph. Every scalar property of `People` (`first_name`, `middle_name`, `last_name`, `ssn`, `birth_date`, `email`, `current_address_id`) is automatically included in the output. A record from `People` can be included multiple times if multiple packages were ordered by that person, or it could be missing entirely if that person is not the customer who ordered any package.

```py
%%pydough
GRAPH.Packages.customer
```

Good Example #3: for every address, obtains all packages that someone who lives at that address has ordered. Every scalar property of `Packages` (`package_id`, `customer_ssn`, `shipping_address_id`, `billing_address_id`, `order_date`, `arrival_date`). Every record from `Packages` should be included at most once since every current occupant has a single address it maps back to, and every package has a single customer it maps back to.

```py
%%pydough
Addresses.current_occupants.packages
```

Bad Example #1: for every address, obtains all people who used to live there. This is invalid because the `Addresses` collection does not have a `former_occupants` property.

```py
%%pydough
Addresses.former_occupants
```

### CALC

TODO

### Contextless Expressions

TODO

### BACK

TODO

## Collection Operators

TODO

### WHERE

TODO

### ORDER_BY

TODO

### TOP_K

TODO

### PARTITION

TODO

### SINGULAR

TODO

### BEST

TODO

### NEXT / PREV

TODO

## Induced Properties

TODO

### Induced Scalar Properties

TODO

### Induced Subcollection Properties

TODO

### Induced Arbitrary Joins

TODO

