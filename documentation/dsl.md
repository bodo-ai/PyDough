# PyDough DSL Spec

This page describes the specification of the PyDough DSL. The specification includes rules of how PyDough code should be structured and the semantics that are used when evaluating PyDough code. Not every feature in the spec is implemented in PyDough as of this time.

## Example Graph

The examples in this document use a metadata graph (named `GRAPH`) with the following collections:
- `People`: records of every known person. Scalar properties: `first_name`, `middle_name`, `last_name`, `ssn`, `birth_date`, `email`, `current_address_id`.
- `Addresses`: records of every known address. Scalar properties: `address_id`, `street_number`, `street_name`, `apartment`, `zip_code`, `city`, `state`.
- `Packages`: records of every known package. Scalar properties: `package_id`, `customer_ssn`, `shipping_address_id`, `billing_address_id`, `order_date`, `arrival_date`, `package_cost`.

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

Good Example #3: for every address, obtains all packages that someone who lives at that address has ordered. Every scalar property of `Packages` (`package_id`, `customer_ssn`, `shipping_address_id`, `billing_address_id`, `order_date`, `arrival_date`, `package_cost`). Every record from `Packages` should be included at most once since every current occupant has a single address it maps back to, and every package has a single customer it maps back to.

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

The examples so far just show selecting all properties from records of a collection. Most of the time, an analytical question will only want a subset of the properties, may want to rename them, and may want to derive new properties via calculated expressions. The way to do this with a CALC term, which is done by following a PyDough collection with parenthesis containing the expressions that should be included.

These expressions can be positional arguments or keyword arguments. Keyword arguments use the name of the keyword as the name of the output expression. Positional arguments use the name of the expression, if one exists, otherwise an arbitrary name is chosen.

The value of one of these terms in a CALC must be expressions that are singular with regards to the current context. That can mean:
- Referencing one of the scalar properties of the current collection.
- Creating a literal.
- Referencing a singular expression of a sub-collection of the current collection that is singular with regards to the current collection.
- Calling a non-aggregation function on more singular expressions.
- Calling an aggregation function on a plural expression.

Once a CALC term is created, all terms of the current collection still exist even if they weren't part of the CALC and can still be referenced, they just will not be part of the final answer. If there are multiple CALC terms, the last one is used to determine what expressions are part of the final answer, so earlier CALCs can be used to derive intermediary expressions. If a CALC includes a term with the same name as an existing property of the collection, the existing name is overridden to include the new term.

A CALC can also be done on the graph itself to create a collection with 1 row and columns corresponding to the properties inside the CALC. This is useful when aggregating an entire collection globally instead of with regards to a parent collection.

Good Example #1: For every person, fetches just their first name & last name.

```py
%%pydough
People(first_name, last_name)
```

Good Example #2: For every package, fetches the package id, the first & last name of the person who ordered it, and the state that it was shipped to. Also includes a field named `secret_key` that is always equal to the string `"alphabet soup"`.

```py
%%pydough
Packages(
    package_id,
    first_name=customer.first_name,
    last_name=customer.last_name,
    shipping_state=shipping_address.state,
    secret_key="alphabet soup",
)
```

Good Example #3: For every person, finds their full name (without the middle name) and counts how many packages they purchased.

```py
%%pydough
People(
    name=JOIN_STRINGS("", first_name, last_name),
    n_packages_ordered=COUNT(packages),
)
```

Good Example #4: For every person, finds their full name including the middle name if one exists, as well as their email. Notice that two CALCs are present, but only the terms from the second one are part of the answer.

```py
%%pydough
People(
    has_middle_name=PRESENT(middle_name)
    full_name_with_middle=JOIN_STRINGS(" ", first_name, middle_name, last_name),
    full_name_without_middle=JOIN_STRINGS(" ", first_name, last_name),
)(
    full_name=IFF(has_middle_name, full_name_with_middle, full_name_without_middle),
    email=email,
)
```

Good Example #4: For every person, finds the year from the most recent package they purchased, and from the first package they ever purchased.

```py
%%pydough
People(
    most_recent_package_year=YEAR(MAX(packages.order_date)),
    first_ever_package_year=YEAR(MIN(packages.order_date)),
)
```

Good Example #5: Count how many people, packages, and addresses are known in the system.

```py
%%pydough
GRAPH(
    n_people=COUNT(People),
    n_packages=COUNT(Packages),
    n_addresses=COUNT(Addresses),
)
```

Good Example #6: For each package, lists the package id and whether the package was shipped to the current address of the person who ordered it.

```py
%%pydough
Packages(
    package_id,
    shipped_to_curr_addr=shipping_address.address_id == customer.current_address.address_id
)
```

Bad Example #1: For each person, lists their first name, last name, and phone number. This is invalid because `People` does not have a property named `phone_number`.

```py
%%pydough
People(first_name, last_name, phone_number)
```

Bad Example #2: For each person, lists their combined first & last name followed by their email. This is invalid because a positional argument is included after a keyword argument.

```py
%%pydough
People(
    full_name=JOIN_STRINGS(" ", first_name, last_name),
    email
)
```

Bad Example #3: For each person, lists the address_id of packages they have ordered. This is invalid because `packages` is a plural property of `People`, so its properties cannot be included in a calc term of `People` unless aggregated.

```py
%%pydough
People(packages.address_id)
```

Bad Example #4: For each person, lists their first/last name followed by the concatenated city/state name of their current address. This is invalid because `current_address` is a plural property of `People`, so its properties cannot be included in a calc term of `People` unless aggregated.

```py
%%pydough
People(
    first_name,
    last_name,
    location=JOIN_STRINGS(", ", current_address.city, current_address.state),
)
```

Bad Example #5: For each address, finds whether the state name starts with `"C"`. This is invalid because it calls the builtin Python `.startswith` string method, which is not supported in PyDough (should have instead used a defined PyDough behavior, like the `STARTSWITH` function).

```py
%%pydough
Addresses(is_c_state=state.startswith("c"))
```

Bad Example #6: For each address, finds the state bird of the state it is in. This is invalid because the `state` property of each record of `Addresses` is a scalar expression, not a subcolleciton, so it does not have any properties that can be accessed with `.` syntax.

```py
%%pydough
Addresses(state_bird=state.bird)
```

Bad Example #7: For each current occupant of each address, lists their first name, last name, and city/state they live in. This is invalid because `city` and `state` are not properties of the current collection (`People`, accessed via `current_occupants` of each record of `Addresses`).

```py
%%pydough
Addresses.current_occupants(first_name, last_name, city, state)
```

Bad Example #8: For each person include their ssn and current address. This is invalid because a collection cannot be a CALC term, and `current_address` is a sub-collection property of `People`. Instead, properties of `current_address` can be accessed.

```py
%%pydough
People(ssn, current_address)
```


### Contextless Expressions

PyDough allows defining snippets of PyDough code out of context that do not make sense until they are later placed within a context. This can be done by writing a contextless expression, binding it to a variable as if it were any other Python expression, then later using it inside of PyDough code. This should always have the same effect as if the PyDough code was written fully in-context, but allows re-using common snippets.

Good Example #1: Same as good example #4 from the CALC section, but written with contextless expressions.

```py
%%pydough
has_middle_name = PRESENT(middle_name)
full_name_with_middle = JOIN_STRINGS(" ", first_name, middle_name, last_name),
full_name_without_middle = JOIN_STRINGS(" ", first_name, last_name),
People(
    full_name=IFF(has_middle_name, full_name_with_middle, full_name_without_middle),
    email=email,
)
```

Good Example #2: for every person, finds the total value of all packages they ordered in February of any year, as well as the number of all such packages, the largest value of any such package, and the percentage of those packages that were specifically on valentine's day

```py
%%pydough
is_february = MONTH(order_date) == 2
february_value = KEEP_IF(package_cost, is_february)
aug_packages = packages(
    is_february=is_february,
    february_value=february_value,
    is_valentines_day=is_february & (DAY(order_date) == 14)
)
n_feb_packages = SUM(aug_packages.is_february)
People(
    ssn,
    total_february_value=SUM(aug_packages.february_value),
    n_february_packages=n_feb_packages,
    most_expensive_february_package=MAX(aug_packages.february_value),
    pct_valentine=n_feb_packages / SUM(aug_packages.is_valentines_day)
)
```

Bad Example #1: Just a contextless expression for a collection without the necessary context for it to make sense.

```py
%%pydough
current_addresses(city, state)
```

Bad Example #2: Just a contextless expression for a scalar expression that has not been placed into a collection for it to make sense.

```py
%%pydough
LOWER(current_occupants.first_name)
```

Bad Example #3: A contextless expression that does not make sense when placed into its context (`People` does not have a property named `package_cost`, so substituting it when `value` is referenced does not make sense).

```py
%%pydough
value = package_cost
People(x=ssn + value)
```

### BACK

Part of the benefit of doing `collection.subcollection` accesses is that properties from the ancestor collection can be accessed from the current collection. This is done via a `BACK` call. Accessing properties from `BACK(n)` can be done to access properties from the n-th ancestor of the current collection. The simplest recommended way to do this is to just access a scalar property of an ancestor in order to include it in the final answer.

Good Example #1: For every address' current occupants, lists their first name last name, and the city/state of the current address they belong to.

```py
%%pydough
Addresses.current_occupants(
    first_name,
    last_name,
    current_city=BACK(1).city,
    current_state=BACK(1).state,
)
```

Good Example #2: Count the total number of cases where a package is shipped to the current address of the customer who ordered it.

```py
%%pydough
package_info = Addresses.current_occupants.packages(
    is_shipped_to_current_addr=shipping_address.address_id == BACK(2).address_id
)
GRAPH(n_cases=SUM(package_info.is_shipped_to_current_addr))
```

Good Example #3: Indicate whether a package is above the average cost for all packages ordered by that customer.

```py
%%pydough
Customers(
    avg_package_cost=AVG(packages.cost)
).packages(
    is_above_avg=cost > BACK(1).avg_package_cost
)
```

Good Example #4: For every customer, indicate what percentage of all packages billed to their current address were purchased by that same customer.

```py
%%pydough
aug_packages = packages(
    include=IFF(billing_address.address_id == BACK(2).address_id, 1, 0)
)
Addresses(
    n_packages=COUNT(packages_billed_to)
).current_occupants(
    ssn,
    pct=100.0 * SUM(aug_packages.include) / BACK(1).n_packages
)
```

Bad Example #1: The `GRAPH` does not have any ancestors, so `BACK(1)` is invalid.

```py
%%pydough
GRAPH(x=BACK(1).foo)
```

Bad Example #2: The 1st ancestor of `People` is `GRAPH` which does not have a term named `bar`.

```py
%%pydough
People(y=BACK(1).bar)
```

Bad Example #3: The 1st ancestor of `People` is `GRAPH` which does not have an ancestor, so there can be no 2nd ancestor of `People`.

```py
%%pydough
People(z=BACK(2).fizz)
```

Bad Example #4: The 1st ancestor of `current_address` is `People` which does not have a term named `phone`.

```py
%%pydough
People.current_address(a=BACK(1).phone)
```

Bad Example #5: Even though `cust_info` has defined `avg_package_cost`, the final expression `Customers.packages(...)` does not have `cust_info` as an ancestor, so it cannot access `BACK(1).avg_package_cost` since its 1st ancestor (`Customers`) does not have any term named `avg_package_cost`.

```py
%%pydough
cust_info = Customers(
    avg_package_cost=AVG(packages.cost)
)
Customers.packages(
    is_above_avg=cost > BACK(1).avg_package_cost
)
```

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

