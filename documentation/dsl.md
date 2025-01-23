# PyDough DSL Spec

This page describes the specification of the PyDough DSL. The specification includes rules of how PyDough code should be structured and the semantics that are used when evaluating PyDough code. Not every feature in the spec is implemented in PyDough as of this time.

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Example Graph](#example-graph)
- [Collections](#collections)
   * [Sub-Collections](#sub-collections)
   * [CALC](#calc)
   * [Contextless Expressions](#contextless-expressions)
   * [BACK](#back)
   * [Expressions](#expressions)
- [Collection Operators](#collection-operators)
   * [WHERE](#where)
   * [ORDER_BY](#order_by)
   * [TOP_K](#top_k)
   * [PARTITION](#partition)
   * [SINGULAR](#singular)
   * [NEXT / PREV](#next-prev)
   * [BEST](#best)
- [Induced Properties](#induced-properties)
   * [Induced Scalar Properties](#induced-scalar-properties)
   * [Induced Subcollection Properties](#induced-subcollection-properties)
   * [Induced Arbitrary Joins](#induced-arbitrary-joins)
- [Larger Examples](#larger-examples)
   * [Example 1: Highest Residency Density States](#example-1-highest-residency-density-states)
   * [Example 2: Yearly Trans-Coastal Shipments](#example-2-yearly-trans-coastal-shipments)
   * [Example 3: Email of Oldest Non-Customer Resident](#example-3-email-of-oldest-non-customer-resident)
   * [Example 4: Outlier Packages Per Month Of 2017](#example-4-outlier-packages-per-month-of-2017)
   * [Example 5: Regression Prediction Of Packages Quantity](#example-5-regression-prediction-of-packages-quantity)

<!-- TOC end -->

<!-- TOC --><a name="example-graph"></a>
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

<!-- TOC --><a name="collections"></a>
## Collections

The simplest PyDough code is scanning an entire collection. This is done by providing the name of the collection in the metadata. However, if that name is already used as a variable, then PyDough will not know to replace the name with the corresponding PyDough object.

**Good Example #1**: obtains every record of the `People` collection. Every scalar property of `People` (`first_name`, `middle_name`, `last_name`, `ssn`, `birth_date`, `email`, `current_address_id`) is automatically included in the output.

```py
%%pydough
People
```

**Good Example #2**: obtains every record of the `Addresses` collection. The `GRAPH.` prefix is optional and implied when the term is a collection name in the graph. Every scalar property of `Addresses` (`address_id`, `street_number`, `street_name`, `apartment`, `zip_code`, `city`, `state`) is automatically included in the output.

```py
%%pydough
GRAPH.Addresses
```

**Good Example #3**: obtains every record of the `Packages` collection. Every scalar property of `Packages` (`package_id`, `customer_ssn`, `shipping_address_id`, `billing_address_id`, `order_date`, `arrival_date`, `package_cost`) is automatically included in the output.

```py
%%pydough
Packages
```

**Bad Example #1**: obtains every record of the `Products` collection (there is no `Products` collection).

```py
%%pydough
Products
```

**Bad Example #2**: obtains every record of the `Addresses` collection (but the name `Addresses` has been reassigned to a variable).

```py
%%pydough
Addresses = 42
Addresses
```

**Bad Example #3**: obtains every record of the `Addresses` collection (but the graph name `HELLO` is the wrong graph name for this example).

```py
%%pydough
HELLO.Addresses
```

<!-- TOC --><a name="sub-collections"></a>
### Sub-Collections

The next step in PyDough after accessing a collection is to access its sub-collections. Using the syntax `collection.subcollection`, you can traverse into every record of `subcollection` for each record in `collection`. This operation may change the cardinality if records of `collection` have multiple associated records in `subcollection`. Additionally, duplicate records may appear in the output if records in `subcollection` are linked to multiple records in `collection`.

**Good Example #1**: for every person, obtains their current address. Every scalar property of `Addresses` (`address_id`, `street_number`, `street_name`, `apartment`, `zip_code`, `city`, `state`) is automatically included in the output. A record from `Addresses` can be included multiple times if multiple different `People` records have it as their current address, or it could be missing entirely if no person has it as their current address.

```py
%%pydough
People.current_addresses
```

**Good Example #2**: For every package, get the person who shipped it. The `GRAPH.` prefix is optional and implied when the term is a collection name in the graph. Every scalar property of `People` (`first_name`, `middle_name`, `last_name`, `ssn`, `birth_date`, `email`, `current_address_id`) is automatically included in the output. A record from `People` can be included multiple times if multiple packages were ordered by that person, or it could be missing entirely if that person is not the customer who ordered any package.

```py
%%pydough
GRAPH.Packages.customer
```

**Good Example #3**: for every address, obtains all packages that someone who lives at that address has ordered. Every scalar property of `Packages` (`package_id`, `customer_ssn`, `shipping_address_id`, `billing_address_id`, `order_date`, `arrival_date`, `package_cost`). Every record from `Packages` should be included at most once since every current occupant has a single address it maps back to, and every package has a single customer it maps back to.

```py
%%pydough
Addresses.current_occupants.packages
```

**Good Example #4**: for every person, obtains all packages they have ordered. Every scalar property of `Packages` (`package_id`, `customer_ssn`, `shipping_address_id`, `billing_address_id`, `order_date`, `arrival_date`, `package_cost`). Every record from `Packages` should be included at most once since every package has a single customer it maps back to.

```py
%%pydough
People.packages
```

**Bad Example #1**: for every address, obtains all people who used to live there. This is invalid because the `Addresses` collection does not have a `former_occupants` property.

```py
%%pydough
Addresses.former_occupants
```

**Bad Example #2**: for every package, obtains all addresses it was shipped to. This is invalid because the `Packages` collection does not have a `shipping_addresses` property (it does have a `shipping_address` property).

```py
%%pydough
Packages.shipping_addresses
```

<!-- TOC --><a name="calc"></a>
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

**Good Example #1**: For every person, fetches just their first name & last name.

```py
%%pydough
People(first_name, last_name)
```

**Good Example #2**: For every package, fetches the package id, the first & last name of the person who ordered it, and the state that it was shipped to. Also includes a field named `secret_key` that is always equal to the string `"alphabet soup"`.

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

**Good Example #3**: For every person, find their full name (without the middle name) and count how many packages they purchased.

```py
%%pydough
People(
    name=JOIN_STRINGS("", first_name, last_name),
    n_packages_ordered=COUNT(packages),
)
```

**Good Example #4**: For every person, find their full name including the middle name if one exists, as well as their email. Notice that two CALCs are present, but only the terms from the second one are part of the answer.

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

**Good Example #5**: For every person, find the year from the most recent package they purchased, and from the first package they ever purchased.

```py
%%pydough
People(
    most_recent_package_year=YEAR(MAX(packages.order_date)),
    first_ever_package_year=YEAR(MIN(packages.order_date)),
)
```

**Good Example #6**: Count how many people, packages, and addresses are known in the system.

```py
%%pydough
GRAPH(
    n_people=COUNT(People),
    n_packages=COUNT(Packages),
    n_addresses=COUNT(Addresses),
)
```

**Good Example #7**: For each package, list the package id and whether the package was shipped to the current address of the person who ordered it.

```py
%%pydough
Packages(
    package_id,
    shipped_to_curr_addr=shipping_address.address_id == customer.current_address.address_id
)
```

**Bad Example #1**: For each person, list their first name, last name, and phone number. This is invalid because `People` does not have a property named `phone_number`.

```py
%%pydough
People(first_name, last_name, phone_number)
```

**Bad Example #2**: For each person, list their combined first & last name followed by their email. This is invalid because a positional argument is included after a keyword argument.

```py
%%pydough
People(
    full_name=JOIN_STRINGS(" ", first_name, last_name),
    email
)
```

**Bad Example #3**: For each person, list the address_id of packages they have ordered. This is invalid because `packages` is a plural property of `People`, so its properties cannot be included in a calc term of `People` unless aggregated.

```py
%%pydough
People(packages.address_id)
```

**Bad Example #4**: For each person, list their first/last name followed by the concatenated city/state name of their current address. This is invalid because `current_address` is a plural property of `People`, so its properties cannot be included in a calc term of `People` unless aggregated.

```py
%%pydough
People(
    first_name,
    last_name,
    location=JOIN_STRINGS(", ", current_address.city, current_address.state),
)
```

**Bad Example #5**: For each address, find whether the state name starts with `"C"`. This is invalid because it calls the builtin Python `.startswith` string method, which is not supported in PyDough (should have instead used a defined PyDough behavior, like the `STARTSWITH` function).

```py
%%pydough
Addresses(is_c_state=state.startswith("c"))
```

**Bad Example #6**: For each address, find the state bird of the state it is in. This is invalid because the `state` property of each record of `Addresses` is a scalar expression, not a subcolleciton, so it does not have any properties that can be accessed with `.` syntax.

```py
%%pydough
Addresses(state_bird=state.bird)
```

**Bad Example #7**: For each current occupant of each address, list their first name, last name, and city/state they live in. This is invalid because `city` and `state` are not properties of the current collection (`People`, accessed via `current_occupants` of each record of `Addresses`).

```py
%%pydough
Addresses.current_occupants(first_name, last_name, city, state)
```

**Bad Example #8**: For each person include their ssn and current address. This is invalid because a collection cannot be a CALC term, and `current_address` is a sub-collection property of `People`. Instead, properties of `current_address` can be accessed.

```py
%%pydough
People(ssn, current_address)
```

**Bad Example #9**: For each person, list their first name, last name, and the sum of the package costs. This is invalid because `SUM` is an aggregation function and cannot be used in a CALC term without specifying the sub-collection it should be applied to.

```py
%%pydough
People(first_name, last_name, total_cost=SUM(package_cost))
```

<!-- TOC --><a name="contextless-expressions"></a>
### Contextless Expressions

PyDough allows defining snippets of PyDough code out of context that do not make sense until they are later placed within a context. This can be done by writing a contextless expression, binding it to a variable as if it were any other Python expression, then later using it inside of PyDough code. This should always have the same effect as if the PyDough code was written fully in-context, but allows re-using common snippets.

**Good Example #1**: Same as good example #4 from the CALC section, but written with contextless expressions.

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

**Good Example #2**: for every person, find the total value of all packages they ordered in February of any year, as well as the number of all such packages, the largest value of any such package, and the percentage of those packages that were specifically on Valentine's day

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

**Bad Example #1**: Just a contextless expression for a collection without the necessary context for it to make sense.

```py
%%pydough
current_addresses(city, state)
```

**Bad Example #2**: Just a contextless expression for a scalar expression that has not been placed into a collection for it to make sense.

```py
%%pydough
LOWER(current_occupants.first_name)
```

**Bad Example #3**: A contextless expression that does not make sense when placed into its context (`People` does not have a property named `package_cost`, so substituting it when `value` is referenced does not make sense).

```py
%%pydough
value = package_cost
People(x=ssn + value)
```

**Bad Example #4**: A contextless expression that does not make sense when placed into its context (`People` does not have a property named `order_date`, so substituting it when `is_february` is referenced does not make sense).

```py
%%pydough
is_february = MONTH(order_date) == 2
People(february=is_february)
```

<!-- TOC --><a name="back"></a>
### BACK

Part of the benefit of doing `collection.subcollection` accesses is that properties from the ancestor collection can be accessed from the current collection. This is done via a `BACK` call. Accessing properties from `BACK(n)` can be done to access properties from the n-th ancestor of the current collection. The simplest recommended way to do this is to just access a scalar property of an ancestor in order to include it in the final answer.

**Good Example #1**: For each address's current occupants, list their first name last name, and the city/state of the current address they belong to.

```py
%%pydough
Addresses.current_occupants(
    first_name,
    last_name,
    current_city=BACK(1).city,
    current_state=BACK(1).state,
)
```

**Good Example #2**: Count the total number of cases where a package is shipped to the current address of the customer who ordered it.

```py
%%pydough
package_info = Addresses.current_occupants.packages(
    is_shipped_to_current_addr=shipping_address.address_id == BACK(2).address_id
)
GRAPH(n_cases=SUM(package_info.is_shipped_to_current_addr))
```

**Good Example #3**: Indicate whether a package is above the average cost for all packages ordered by that customer.

```py
%%pydough
Customers(
    avg_package_cost=AVG(packages.cost)
).packages(
    is_above_avg=cost > BACK(1).avg_package_cost
)
```

**Good Example #4**: For every customer, indicate what percentage of all packages billed to their current address were purchased by that same customer.

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

**Bad Example #1**: The `GRAPH` does not have any ancestors, so `BACK(1)` is invalid.

```py
%%pydough
GRAPH(x=BACK(1).foo)
```

**Bad Example #2**: The 1st ancestor of `People` is `GRAPH` which does not have a term named `bar`.

```py
%%pydough
People(y=BACK(1).bar)
```

**Bad Example #3**: The 1st ancestor of `People` is `GRAPH` which does not have an ancestor, so there can be no 2nd ancestor of `People`.

```py
%%pydough
People(z=BACK(2).fizz)
```

**Bad Example #4**: The 1st ancestor of `current_address` is `People` which does not have a term named `phone`.

```py
%%pydough
People.current_address(a=BACK(1).phone)
```

**Bad Example #5**: Even though `cust_info` has defined `avg_package_cost`, the final expression `Customers.packages(...)` does not have `cust_info` as an ancestor, so it cannot access `BACK(1).avg_package_cost` since its 1st ancestor (`Customers`) does not have any term named `avg_package_cost`.

```py
%%pydough
cust_info = Customers(
    avg_package_cost=AVG(packages.cost)
)
Customers.packages(
    is_above_avg=cost > BACK(1).avg_package_cost
)
```

<!-- TOC --><a name="expressions"></a>
### Expressions

So far, many different kinds of expressions have been noted in the examples for CALC, BACK, and contextless expressions. The following are examples & explanations of the various types of valid expressions:

```py
# Referencing scalar properties of the current collection
People(
    first_name,
    last_name
)

# Referencing scalar properties of a singular sub-collection
People(
    current_state=current_address.state,
    current_state=current_address.state,
)

# Referencing scalar properties of an ancestor collection
Addresses.current_occupants.packages(
    customer_email=BACK(1).email,
    customer_zip_code=BACK(2).zip_code,
)

# Invoking normal functions/operations on other singular data
Customers(
    lowered_name=LOWER(name),
    normalized_birth_month=MONTH(birth_date) - 1,
    lives_in_c_state=STARTSWITH(current_address.state, "C"),
)

# Supported Python literals:
# - integers
# - floats
# - strings
# - booleans
# - None
# - decimal.Decimal
# - pandas.Timestamp
# - datetime.date
# - lists/tuples of literals
from pandas import Timestamp
from datetime import date
from decimal import Decimal
Customers(
    a=0,
    b=3.14,
    c="hello world",
    d=True,
    e=None,
    f=decimal.Decimal("2.718281828"),
    g=Timestamp("now"),
    h=date(2024, 1, 1),
    i=[1, 2, 4, 8, 10],
    j=("SMALL", "LARGE"),
)

# Invoking aggregation functions on plural data
Customers(
    n_packages=COUNT(packages),
    home_has_had_packages_billed=HAS(current_address.billed_packages),
    avg_package_cost=AVG(packages.package_cost),
    n_states_shipped_to=NDISTINCT(packages.shipping_address.state),
    most_recent_package_ordered=MAX(packages.order_date),
)

# Invoking window functions on singular
Customers(
    cust_ranking=RANKING(by=COUNT(packages).DESC()),
    cust_percentile=PERCENTILE(by=COUNT(packages).DESC()),
)
```

See [the list of PyDough functions](funcitons.md) to see all of the builtin functions & operators that can be called in PyDough.

<!-- TOC --><a name="collection-operators"></a>
## Collection Operators

So far all of the examples shown have been about accessing collections/sub-collections and deriving expression in terms of the current context, child contexts, and ancestor context. PyDough has other operations that access/create/augment collections.

<!-- TOC --><a name="where"></a>
### WHERE

A core PyDough operation is the ability to filter the records of a collection. This is done by appending a PyDough collection with `.WHERE(cond)` where `cond` is any expression that could have been placed in a `CALC` term and should have a True/False value. Every record where `cond` evaluates to True will be preserved, and the rest will be dropped from the answer. The terms in the collection are unchanged by the `WHERE` clause, since the only change is which records are kept/dropped.

**Good Example #1**: For every person who has a middle name and and email that ends with `"gmail.com"`, fetches their first name and last name.

```py
%%pydough
People.WHERE(PRESENT(middle_name) & ENDSWITH(email, "gmail.com"))(first_name, last_name)
```

**Good Example #2**: For every package where the package cost is greater than 100, fetches the package id and the state it was shipped to.

```py
%%pydough
Packages.WHERE(package_cost > 100)(package_id, shipping_state=shipping_address.state)
```

**Good Example #3**: For every person who has ordered more than 5 packages, fetches their first name, last name, and email.

```py
%%pydough
People(first_name, last_name, email).WHERE(COUNT(packages) > 5)
```

**Good Example #4**: Find every person whose most recent order was shipped in the year 2023, and list all properties of that person. 

```py
%%pydough
People.WHERE(YEAR(MAX(packages.order_date)) == 2023)
```

**Good Example #5**: Count how many packages were ordered in January of 2018. [See here](functions.md#logical) for more details on the valid/invalid use of logical operations in Python.

```py
%%pydough
packages_jan_2018 = Packages.WHERE(
    (YEAR(order_date) == 2018) & (MONTH(order_date) == 1)
)
GRAPH(n_jan_2018=COUNT(selected_packages))
```

**Good Example #6**: Counts how many people have don't have a first or last name that starts with A. [See here](functions.md#logical) for more details on the valid/invalid use of logical operations in Python.

```py
%%pydough
selected_people = People.WHERE(
    ~STARTSWITH(first_name, "A") & ~STARTSWITH(first_name, "B") 
)
GRAPH(n_people=COUNT(selected_people))
```

**Good Example #7**: Counts how many people have a gmail or yahoo account. [See here](functions.md#logical) for more details on the valid/invalid use of logical operations in Python.

```py
%%pydough
gmail_or_yahoo = People.WHERE(
    ENDSWITH(email, "@gmail.com") | ENDSWITH(email, "@yahoo.com") 
)
GRAPH(n_gmail_or_yahoo=COUNT(gmail_or_yahoo))
```

**Good Example #8**: Counts how many people were born in the 1980s. [See here](functions.md#comparisons) for more details on the valid/invalid use of comparisons in Python.

```py
%%pydough
eighties_babies = People.WHERE(
    (1980 <= YEAR(birth_date)) & (YEAR(birth_date) < 1990)
)
GRAPH(n_eighties_babies=COUNT(eighties_babies))
```

**Good Example #9**: Find every person whose has sent a package to Idaho.

```py
%%pydough
People.WHERE(HAS(packages.WHERE(shipping_address.state == "ID")))
```

**Good Example #10**: Find every person whose did not order a package in 2024.

```py
%%pydough
People.WHERE(HASNOT(packages.WHERE(YEAR(order_date) == 2024)))
```

**Bad Example #1**: For every person, fetches their first name and last name only if they have a phone number. This is invalid because `People` does not have a property named `phone_number`.

```py
%%pydough
People.WHERE(PRESENT(phone_number))(first_name, last_name)
```

**Bad Example #2**: For every package, fetches the package id only if the package cost is greater than 100 and the shipping state is Texas. This is invalid because `and` is used instead of `&`. [See here](functions.md#logical) for more details on the valid/invalid use of logical operations in Python.

```py
%%pydough
Packages.WHERE((package_cost > 100) and (shipping_address.state == "TX"))(package_id)
```

**Bad Example #3**: For every package, fetches the package id only if the package is either being shipped from Pennsylvania or to Pennsylvania. This is invalid because `or` is used instead of `|`. [See here](functions.md#logical) for more details on the valid/invalid use of logical operations in Python.

```py
%%pydough
Packages.WHERE((customer.current_address.state == "PA") or (shipping_address.state == "PA"))(package_id)
```

**Bad Example #4**: For every package, fetches the package id only if the customer's first name does not start with a J. This is invalid because `not` is used instead of `~`. [See here](functions.md#logical) for more details on the valid/invalid use of logical operations in Python.

```py
%%pydough
Packages.WHERE(not STARTSWITH(customer.first_name, "J"))(package_id)
```

**Bad Example #5**: For every package, fetches the package id only if the package was ordered between February and May. [See here](functions.md#comparisons) for more details on the valid/invalid use of comparisons in Python.

```py
%%pydough
Packages.WHERE(2 <= MONTH(arrival_date) <= 5)(package_id)
```

**Bad Example #6**: Obtain every person whose packages were shipped in the month of June. This is invalid because `packages` is a plural property of `People`, so `MONTH(packages.order_date) == 6` is a plural expression with regards to `People` that cannot be used as a filtering condition. 

```py
%%pydough
People.WHERE(MONTH(packages.order_date) == 6)
```

<!-- TOC --><a name="order_by"></a>
### ORDER_BY

Another operation that can be done onto PyDough collections is sorting them. This is done by appending a collection with `.ORDER_BY(...)` which will order the collection by the collation terms between the parenthesis. The collation terms must be 1+ expressions that can be inside of a CALC term (singular expressions with regards to the current context), each decorated with information making it usable as a collation.

An expression becomes a collation expression when it is appended with `.ASC()` (indicating that the expression should be used to sort in ascending order) or `.DESC()` (indicating that the expression should be used to sort in descending order). Both `.ASC()` and `.DESC()` take in an optional argument `na_pos` indicating where to place null values. This keyword argument can be either `"first"` or `"last"`, and the default is `"first"` for `.ASC()` and `"last"` for `.DESC()`. The way the sorting works is that it orders by hte first collation term provided, and in cases of ties it moves on to the second collation term, and if there are ties in that it moves on to the third, and so on until there are no more terms to sort by, at which point the ties are broken arbitrarily.

If there are multiple `ORDER_BY` terms, the last one is the one that takes precedence. The terms in the collection are unchanged by the `ORDER_BY` clause, since the only change is the order of the records.

> [!WARNING]
> In the current version of PyDough, the behavior when the expressions inside an `ORDER_BY` clause are not collation expressions with `.ASC()` or `.DESC()` is undefined/unsupported.

**Good Example #1**: Orders every person alphabetically by last name, then first name, then middle name (people with no middle name going last).

```py
%%pydough
People.ORDER_BY(last_name.ASC(), first_name.ASC(), middle_name.ASC(na_pos="last"))
```

**Good Example #2**: For every person list their ssn & how many packages they have ordered, and orders them from highest number of orders to lowest, breaking ties in favor of whoever is oldest. 

```py
%%pydough
People(
    ssn, n_packages=COUNT(packages).DESC()
).ORDER_BY(
    n_packages.DESC(), birth_date.ASC()
)
```

**Good Example #3**: Find every address that has at least 1 person living in it and sorts them highest-to-lowest by number of occupants, with ties broken by address id in ascending order. 

```py
%%pydough
Addresses.WHERE(
    HAS(current_occupants)
).ORDER_BY(
    COUNT(current_occupants).DESC(), address_id.ASC()
)
```

**Good Example #4**: Sorts every person alphabetically by the state they live in, then the city they live in, then by their ssn. People without a current address should go last.

```py
%%pydough
People.ORDER_BY(
    current_address.state.ASC(na_pos="last"),
    current_address.city.ASC(na_pos="last"),
    ssn.ASC(),
)
```

**Good Example #5**: Same as good example #4, but written so it only includes people who are current occupants of an address in Ohio.

```py
%%pydough
Addresses.WHERE(
    state == "OHIO"
).current_occupants.ORDER_BY(
    BACK(1).state.ASC(),
    BACK(1).city.ASC(),
    ssn.ASC(),
)
```

**Good Example #6**: Find all people who are in the top 1% of customers according to number of packages ordered.

```py
%%pydough
People.WHERE(PERCENTILE(by=COUNT(packages).ASC()) == 100)
```

**Bad Example #1**: Sorts each person by their account balance in descending order. This is invalid because the `People` collection does not have an `account_balance` property.

```py
%%pydough
People.ORDER_BY(account_balance.DESC())
```

**Bad Example #2**: Sorts each address by the birth date date of the people who live there. This is invalid because `current_occupants` is a plural property of `Addresses`, so `current_occupants.birth_date` is plural and cannot be used as an ordering term unless aggregated.

```py
%%pydough
Addresses.ORDER_BY(current_occupants.ASC())
```

**Bad Example #3**: Same as good example #5, but incorrect because `BACK(2)` is used, and `BACK(2)` refers to the 2nd ancestor of `current_occupants` which is `GRAPH`, which does not have any properties named `state` or `city`.

```py
%%pydough
Addresses.WHERE(
    state == "OHIO"
).current_occupants.ORDER_BY(
    BACK(2).state.ASC(),
    BACK(2).city.ASC(),
    ssn.ASC(),
)
```

**Bad Example #4**: Same as bad example #3, but incorrect because `BACK(3)` is used, and `BACK(3)` refers to the 3rd ancestor of `current_occupants` which does not exist because the 2nd ancestor is `GRAPH`, which does not have any ancestors.

```py
%%pydough
Addresses.WHERE(
    state == "OHIO"
).current_occupants.ORDER_BY(
    BACK(2).state.ASC(),
    BACK(2).city.ASC(),
    ssn.ASC(),
)
```

**Bad Example #5**: Sorts every person by their first name. This is invalid because no `.ASC()` or `.DESC()` term is provided.

```py
%%pydough
People.ORDER_BY(first_name)
```

**Bad Example #6**: Sorts every person. This is invalid because no collation terms are provided.

```py
%%pydough
People.ORDER_BY()
```

<!-- TOC --><a name="top_k"></a>
### TOP_K

A similar operation to `ORDER_BY` is `TOP_K`. The `TOP_K` operation also sorts a collection, but then uses the ordered results in order to pick the first `k`, values, where `k` is a provided constant.

The syntax for this is `.TOP_K(k, by=...)` where `k` is a positive integer and the `by` clause is either a single collation term (as seen in `ORDER_BY`) or an iterable of collation terms (e.g. a list or tuple). The same restrictions as `ORDER_BY` apply to `TOP_K` regarding their collation terms.

The terms in the collection are unchanged by the `TOP_K` clause, since the only change is the order of the records and which ones are kept/dropped.

**Good Example #1**: Find the 10 people who have ordered the most packages, including their first/last name, birth date, and the number of packages. If there is a tie, break it by the lowest ssn.

```py
%%pydough
People(
    first_name,
    last_name,
    birth_date,
    n_packages=COUNT(packages)
).TOP_K(10, by=(n_packages.DESC(), ssn.ASC()))
```

**Good Example #2**: Find the 5 most recently shipped packages, with ties broken arbitrarily.

```py
%%pydough
Packages.TOP_K(5, by=order_date.DESC())
```

**Good Example #3**: Find the 100 addresses that have most recently had packages either shipped or billed to them, breaking ties arbitrarily.

```py
%%pydough
default_date = datetime.date(1970, 1, 1)
most_recent_ship = DEFAULT_TO(MAX(packages_shipped.order_date), default_date)
most_recent_bill = DEFAULT_TO(MAX(packages_billed.order_date), default_date)
most_recent_package = IFF(most_recent_ship < most_recent_bill, most_recent_ship, most_recent_bill)
Addresses.TOP_K(10, by=most_recent_package.DESC())
```

**Good Example #4**: Find the top 3 people who have spent the most money on packages, including their first/last name, and the total cost of all of their packages.

```py
%%pydough
People(
    first_name,
    last_name,
    total_package_cost=SUM(packages.package_cost)
).TOP_K(3, by=total_package_cost.DESC())
```

**Bad Example #1**: Find the 5 people with the lowest GPA. This is invalid because the `People` collection does not have a `gpa` property.

```py
%%pydough
People.TOP_K(5, by=gpa.ASC())
```

**Bad Example #2**: Find the 25 addresses with the earliest packages billed to them, by arrival date. This is invalid because `packages_billed` is a plural property of `Addresses`, so `packages_billed.arrival_date` cannot be used as a collation expression for `Addresses`.

```py
%%pydough
Addresses.packages_billed(25, by=gpa.packages_billed.arrival_date())
```

**Bad Example #3**: Find the top 100 people currently living in the city of San Francisco. This is invalid because the `by` clause is absent.

```py
%%pydough
People.WHERE(
    current_address.city == "San Francisco"
).TOP_K(100)
```

**Bad Example #4**: Find the top packages by highest value. This is invalid because there is no `k` value.

```py
%%pydough
Packages.TOP_K(by=package_cost.DESC())
```

**Bad Example #5**: Find the top 300 addresses. This is invalid because the `by` clause is empty

```py
%%pydough
Addresses.TOP_K(300, by=())
```

**Bad Example #6**: Find the 1000 people by birth date. This is invalid because the collation term does not have `.ASC()` or `.DESC()`.

```py
%%pydough
People.TOP_K(1000, by=birth_date)
```

<!-- TOC --><a name="partition"></a>
### PARTITION

The `PARTITION` operation is used to create a new collection by partitioning the records of another collection based on 1+ partitioning terms. Every unique combination values of those partitioning terms corresponds to a single record in the new collection. The terms of the new collection are the partitioning terms, and a single sub-collection mapping back to the bucketed terms of the original data.

The syntax for this is `PARTITION(data, name="...", by=...)`. The `data` argument is the PyDough collection that is to be partitioned. The `name` argument is a string indicating the name that is to be used when accessing the partitioned data, and the `by` argument is either a single partitioning key, or an iterable of 1+ partitioning keys.

> [!WARNING]
> PyDough currently only supports using references to scalar expressions from the `data` collection itself as partition keys, not an ancestor term, or a term from a child collection, or the result of a function call.

If the partitioned data is accessed, its original ancestry is lost. Instead, it inherits the ancestry of the `PARTITION` clause, so `BACK(1)` is the `PARTITION` clause, and `BACK(2)` is the ancestor of the partition clause (the default is the entire graph, just like collections).

The ancestry of the `PARTITION` clause can be changed by prepending it with another collection, separated by a dot. However, this is currently only supported in PyDough when the collection before the dot is just an augmented version of the graph context, as opposed to another collection (e.g. `GRAPH(x=42).PARTITION(...)` is supported, but `People.PARTITION(...)` is not).

**Good Example #1**: Find every unique state.

```py
%%pydough
PARTITION(Addresses, name="addrs", by=state)(state)
```

**Good Example #2**: For every state, count how many addresses are in that state.

```py
%%pydough
PARTITION(Addresses, name="addrs", by=state)(
    state,
    n_addr=COUNT(addrs)
)
```

**Good Example #3**: For every city/state, count how many people live in that city/state.

```py
%%pydough
PARTITION(Addresses, name="addrs", by=(city, state))(
    state,
    city,
    n_people=COUNT(addrs.current_occupants)
)
```

**Good Example #4**: Find the top 5 years with the most people born in that year who have yahoo email accounts, listing the year and the number of people.

```py
%%pydough
yahoo_people = People(
    birth_year=YEAR(birth_date)
).WHERE(ENDSWITH(email, "@yahoo.com"))
PARTITION(yahoo_people, name="yah_ppl", by=birth_year)(
    birth_year,
    n_people=COUNT(yah_ppl)
).TOP_K(5, by=n_people.DESC())
```

**Good Example #4**: For every year/month, find all packages that were below the average cost of all packages ordered in that year/month.

```py
%%pydough
package_info = Packages(order_year=YEAR(order_date), order_month=MONTH(order_date))
PARTITION(package_info, name="packs", by=(order_year, order_month))(
    avg_package_cost=AVG(packs.package_cost)
).packs.WHERE(
    package_cost < BACK(1).avg_package_cost
)
```

**Good Example #5**: For every customer, find the percentage of all orders made by current occupants of that city/state made by that specific customer. Includes the first/last name of the person, the city/state they live in, and the percentage.

```py
%%pydough
PARTITION(Addresses, name="addrs", by=(city, state))(
    total_packages=COUNT(addrs.current_occupants.packages)
).addrs.current_occupants(
    first_name,
    last_name,
    city=BACK(1).city,
    state=BACK(1).state,
    pct_of_packages=100.0 * COUNT(packages) / BACK(2).total_packages,
)
```

**Good Example #6**: Identifies the states whose current occupants account for at least 1% of all packages purchased. List the state and the percentage.

```py
%%pydough
GRAPH(
    total_packages=COUNT(Packages)
).PARTITION(Addresses, name="addrs", by=state)(
    state,
    pct_of_packages=100.0 * COUNT(addrs.current_occupants.package) / BACK(1).packages
).WHERE(pct_of_packages >= 1.0)
```

**Good Example #7**: Identifies which months of the year have numbers of packages shipped in that month that are above the average for all months.

```py
%%pydough
pack_info = Packages(order_month=MONTH(order_date))
month_info = PARTITION(pack_info, name="packs", by=order_month)(
    n_packages=COUNT(packs)
)
GRAPH(
    avg_packages_per_month=AVG(month_info.n_packages)
).PARTITION(pack_info, name="packs", by=order_month)(
    month,
).WHERE(COUNT(packs) > BACK(1).avg_packages_per_month)
```

**Good Example #8**: Find the 10 most frequent combinations of the state that the person lives in and the first letter of that person's name.

```py
%%pydough
people_info = Addresses.current_occupants(
    state=BACK(1).state,
    first_letter=first_name[:1],
)
PARTITION(people_info, name="ppl", by=(state, first_letter))(
    state,
    first_letter,
    n_people=COUNT(ppl),
).TOP_K(10, by=n_people.DESC())
```

**Good Example #9**: Same as good example #8, but written differently so it will include people without a current address (their state is listed as `"N/A"`).

```py
%%pydough
people_info = People(
    state=DEFALT_TO(current_address.state, "N/A"),
    first_letter=first_name[:1],
)
PARTITION(people_info, name="ppl", by=(state, first_letter))(
    state,
    first_letter,
    n_people=COUNT(ppl),
).TOP_K(10, by=n_people.DESC())
```

**Bad Example #1**: Partitions a collection `Products` that does not exist in the graph.

```py
%%pydough
PARTITION(Products, name="p", by=product_type)
```

**Bad Example #2**: Does not provide a valid `name` when partitioning `Addresses` by the state.

```py
%%pydough
PARTITION(Addresses, by=state)
```

**Bad Example #3**: Does not provide a `by` argument to partition `People`.

```py
%%pydough
PARTITION(People, name="ppl")
```

**Bad Example #4**: Count how many packages were ordered in each year. Invalid because `YEAR(order_date)` is not allowed ot be used as a partition term (it must be placed in a CALC so it is accessible as a named reference).

```py
%%pydough
PARTITION(Packages, name="packs", by=YEAR(order_date))(
    n_packages=COUNT(packages)
)
```

**Bad Example #5**: Count how many people live in each state. Invalid because `current_address.state` is not allowed to be used as a partition term (it must be placed in a CALC so it is accessible as a named reference).

```py
%%pydough
PARTITION(People, name="ppl", by=current_address.state)(
    n_packages=COUNT(packages)
)
```

**Bad Example #6**: Invalid version of good example #8 that did not use a CALC to get rid of the `BACK` or `first_name[:1]`, which cannot be used as partition terms.

```py
%%pydough
PARTITION(Addresses.current_occupants, name="ppl", by=(BACK(1).state, first_name[:1]))(
    BACK(1).state,
    first_name[:1],
    n_people=COUNT(ppl),
).TOP_K(10, by=n_people.DESC())
```

**Bad Example #7**: Partitions people by their birth year to find the number of people born in each year. Invalid because the `email` property is referenced, which is not one of the properties accessible by the partition.

```py
%%pydough
PARTITION(People(birth_year=YEAR(birth_date)), name="ppl", by=birth_year)(
    birth_year,
    email,
    n_people=COUNT(ppl)
)
```

**Bad Example #7**: For each person & year, count how many times that person ordered a packaged in that year. This is invalid because doing `.PARTITION` after `People` is unsupported, since `People` is not a graph-level collection like `GRAPH(...)`.

```py
%%pydough
People.PARTITION(packages(year=YEAR(order_date)), name="p", by=year)(
    ssn=BACK(1).ssn,
    year=year,
    n_packs=COUNT(p)
)
```

**Bad Example #8**: Partitions each address' current occupants by their birth year to get the number of people per birth year. This is invalid because the example includes a field `BACK(2).bar` which does not exist because the first ancestor of the partition is `GRAPH`, which does not have a second ancestor.

```py
%%pydough
people_info = Addresses.current_occupants(birth_year=YEAR(birth_date))
GRAPH.PARTITION(people_info, name="p", by=birth_year)(
    birth_year,
    n_people=COUNT(p),
    foo=BACK(2).bar,
)
```

**Bad Example #9**: Partitions each address' current occupants by their birth year and filters to only include people born in years where at least 10000 people were born, then gets more information of people from those years. This is invalid because after accessing `.ppl`, the term `BACK(1).state` is used. This is not valid because even though the data that `.ppl` refers to (`people_info`) has access to `BACK(1).state`, that ancestry information was lost after partitioning `people_info`. Instead, `BACK(1)` refers to the `PARTITION` clause, which does not have a `state` field.

```py
%%pydough
people_info = Addresses.current_occupants(birth_year=YEAR(birth_date))
GRAPH.PARTITION(people_info, name="ppl", by=birth_year).WHERE(
    COUNT(p) >= 10000
).ppl(
    first_name,
    last_name,
    state=BACK(1).state,
)
```

<!-- TOC --><a name="singular"></a>
### SINGULAR

> [!IMPORTANT]
> This feature has not yet been implemented in PyDough

Certain PyDough operations, such as specific filters, can cause plural data to become singular. In this case, PyDough will still ban the plural data from being treated as singular unless the `.SINGULAR()` modifier is used to tell PyDough that the data should be treated as singular. It is very important that this only be used if the user is certain that the data will be singular, since otherwise it can result in undefined behavior when the PyDough code is executed.

**Good Example #1**: Accesses the package cost of the most recent package ordered by each person. This is valid because even though `.packages` is plural, the filter done on it will ensure that there is only one record for each record of `People`, so `.SINGULAR()` is valid.

```py
%%pydough
most_recent_package = packages.WHERE(
    RANKING(by=order_date.DESC(), levels=1) == 1
).SINGULAR()
People(
    ssn,
    first_name,
    middle_name,
    last_name,
    most_recent_package_cost=most_recent_package.package_cost
)
```

**Good Example #2**: Accesses the email of the current occupant of each address that has the name `"John Smith"` (no middle name). This is valid if it is safe to assume that each address only has one current occupant named `"John Smith"` without a middle name.

```py
%%pydough
js = current_occupants.WHERE(
    (first_name == "John") &  
    (last_name == "Smith") & 
    ABSENT(middle_name)
).SINGULAR()
Addresses(
    address_id,
    john_smith_email=DEFAULT_TO(js.email, "NO JOHN SMITH LIVING HERE")
)
```

<!-- TOC --><a name="next-prev"></a>
### NEXT / PREV

> [!IMPORTANT]
> This feature has not yet been implemented in PyDough

In PyDough, it is also possible to access data from other records in the same collection that occur before or after the current record, when all the records are sorted. Similar to how `BACK(n)` can be used as a collection to access terms from an ancestor context, `PREV(n, by=...)` can be used to access terms from another record of the same context, specifically the record obtained by ordering by the `by` terms then looking for the record `n` entries before the current record. Similarly, `NEXT(n, ...)` is the same as `PREV(-n, ...)`.

The arguments to `NEXT` and `PREV` are as follows:
- `n` (optional): how many records before/after the current record to look. The default value is 1.
- `by` (required): the collation terms used to sort the data. Must be either a single collation term, or an iterable of 1+ collation terms.
- `levels` (optional): same as window functions such as `RANKING` or `PERCENTILE`, documented in the [functions list](functions.md).

If the entry `n` records before/after the current entry does not exist, then accessing anything from it returns null. Anything that can be done to the current context can also be done to the `PREV`/`NEXT` call (e.g. aggregating data from a plural sub-collection).

**Good Example #1**: For each package, find whether it was ordered by the same customer as the most recently ordered package before it.

```py
%%pydough
Packages(
    package_id,
    same_customer_as_prev_package=customer_ssn == PREV(by=order_date.ASC()).ssn
)
```

**Good Example #2**: Find the average number of hours between every package ordered by every customer.

```py
%%pydough
prev_package = PREV(by=order_date.ASC(), levels=1)
package_deltas = packages(
    hour_difference=DATEDIFF('hours', order_date, prev_package.order_date)
)
Customers(
    ssn,
    avg_hours_between_purchases=AVG(package_deltas.hour_difference)
)
```

**Good Example #3**: Find out for each customer whether, if they were sorted by number of packages ordered, whether they live in the same state as any of the 3 people below them on the list.

```py
%%pydough
first_after = NEXT(1, by=COUNT(packages).DESC())
second_after = NEXT(2, by=COUNT(packages).DESC())
third_after = NEXT(3, by=COUNT(packages).DESC())
Customers(
    ssn,
    same_state_as_order_neighbors=(
        DEFAULT_TO(current_address.state == first_after.current_address.state, False) | 
        DEFAULT_TO(current_address.state == second_after.current_address.state, False) | 
        DEFAULT_TO(current_address.state == third_after.current_address.state, False) 
    )
)
```

**Bad Example #1**: Find the number of hours between each package and the previous package. This is invalid because the `by` argument is missing

```py
%%pydough
Packages(
    hour_difference=DATEDIFF('hours', order_date, PREV().order_date)
)
```

**Bad Example #2**: Find the number of hours between each package and the next package. This is invalid because the `by` argument is empty.

```py
%%pydough
Packages(
    hour_difference=DATEDIFF('hours', order_date, NEXT(by=()).order_date)
)
```

**Bad Example #3**: Find the number of hours between each package and the 5th-previous package. This is invalid because the `by` argument is not a collation.

```py
%%pydough
Packages(
    hour_difference=DATEDIFF('hours', order_date, PREV(5, by=order_date).order_date)
)
```

**Bad Example #4**: Find the number of hours between each package and a subsequent package. This is invalid because the `n` argument is not an integer.

```py
%%pydough
Packages(
    hour_difference=DATEDIFF('hours', order_date, NEXT("ten", by=order_date.ASC()).order_date)
)
```

**Bad Example #5**: Invalid usage of `PREV` that is used as-is without accessing any of its fields.

```py
%%pydough
Packages(
    hour_difference=DATEDIFF('hours', order_date, PREV(1, by=order_date.ASC()))
)
```

**Bad Example #6**: Find the number of hours between each package and the previous package. This invalid because a property `.odate` is accessed that does not exist in the collection, therefore it doesn't exist in `PREV` either.

```py
%%pydough
Packages(
    hour_difference=DATEDIFF('hours', order_date, PREV(1, by=order_date.ASC()).odate)
)
```

**Bad Example #7**: Invalid use of `PREV` that is invoked with `.` syntax, like a subcollection.

```py
%%pydough
Packages.PREV(order_date.ASC())
```

**Bad Example #8**: Find the number of hours between each package and the previous package ordered by the customer. This invalid because the `levels` value is too large, since only 2 ancestor levels exist in `Customers.packages` (the graph, and `Customers`):

```py
%%pydough
Customers.packages(
    hour_difference=DATEDIFF('hours', order_date, PREV(1, by=order_date.ASC(), levels=5).order_date)
)
```

<!-- TOC --><a name="best"></a>
### BEST

> [!IMPORTANT]
> This feature has not yet been implemented in PyDough

PyDough supports identifying a specific record from a sub-collection that is optimal with regards to some metric, per-record of the current collection. This is done by using `BEST` instead of directly accessing the sub-collection. The first argument to `BEST` is the sub-collection to be accessed, and the second is a `by` argument used to find the optimal record of the sub-collection. The rules for the `by` argument are the same as `PREV`, `NEXT`, `TOP_K`, etc.: it must be either a single collation term, or an iterable of 1+ collation terms.

A call to `BEST` can either be done with `.` syntax, to step from a parent collection to a child collection, or can be a freestanding accessor used inside of a collection operator, just like `BACK`, `PREV` or `NEXT`. For example, both `Parent.BEST(child, by=...)` and `Parent(x=BEST(child, by=...).y)` are allowed.

The original ancestry of the sub-collection is intact. So, if doing `A.BEST(b.c.d, by=...)`, `BACK(1)` revers to `c`, `BACK(2)` refers to `b` and `BACK(3)` refers to `A`.

Additional keyword arguments can be supplied to `BEST` that change its behavior:
- `allow_ties` (default=False): if True, changes the behavior to keep all records of the sub-collection that share the optimal values of the collation terms. If `allow_ties` is True, the `BEST` clause is no longer singular.
- `n_best=True`(defaults=1): if an integer greater than 1, changes the behavior to keep the top `n_best` values of the sub-collection for each record of the parent collection (fewer if `n_best` records of the sub-collection do not exist). If `n_best` is greater than 1, the `BEST` clause is no longer singular. NOTE: `n_best` cannot be greater than 1 at the same time that `allow_ties` is True.

**Good Example #1**: Find the package id & zip code the package was shipped to for every package that was the first-ever purchase for the customer.

```py
%%pydough
Customers.BEST(packages, by=order_date.ASC())(
    package_id,
    shipping_address.zip_code
)
```

**Good Example #2**: For each customer, list their ssn and the cost of the most recent package they have purchased.

```py
%%pydough
Customers(
    ssn,
    most_recent_cost=BEST(packages, by=order_date.DESC()).package_cost
)
```

**Good Example #3**: Find the address in the state of New York with the most occupants, ties broken by address id. Note: the `GRAPH.` prefix is optional in this case, since it is implied if there is no prefix to the `BEST` call.

```py
%%pydough
addr_info = Addresses.WHERE(
    state == "NY"
)(address_id, n_occupants=COUNT(current_occupants))
GRAPH.BEST(addr_info, by=(n_occupants.DESC(), address_id.ASC()))
```

**Good Example #4**: For each customer, find the number of people currently living in the address that they most recently shipped a package to.

```py
%%pydough
most_recent_package = BEST(packages, by=order_date.DESC())
Customers(
    ssn,
    n_occ_most_recent_addr=COUNT(most_recent_package.shipping_address.current_occupants)
)
```

**Good Example #5**: For each address that has occupants, list out the first/last name of the person living in that address who has ordered the most packages, breaking ties in favor of the person with the smaller social security number. Also includes the city/state of the address, the number of people who live there, and the number of packages that person ordered.

```py
%%pydough
Addresses.WHERE(HAS(current_occupants))(
    n_occupants=COUNT(current_occupants)
).BEST(
    current_occupants(n_orders=COUNT(packages)),
    by=(n_orders.DESC(), ssn.ASC())
)(
    first_name,
    last_name,
    n_orders,
    n_living_in_same_addr=BACK(1).n_occupants,
    city=BACK(1).city,
    state=BACK(1).state,
)
```

**Good Example #6**: For each person, find the total value of the 5 most recent packages they ordered.

```py
%%pydough
five_most_recent = BEST(packages, by=order_date.DESC(), n_best=5)
People(
    ssn,
    value_most_recent_5=SUM(five_most_recent.package_cost)
)
```

**Good Example #7**: For each address, find the package most recently ordered by one of the current occupants of that address, including the email of the occupant who ordered it and the address' id. Notice that `BACK(1)` refers to `current_occupants` and `BACK(2)` refers to `Addresses` as if the packages were accessed as `Addresses.current_occupants.packages` instead of using `BEST`.

```py
%%pydough
most_recent_package = BEST(current_occupants.packages, by=order_date.DESC())
Addresses.most_recent_package(
    address_id=BACK(2).address_id,
    cust_email=BACK(1).email,
    package_id=package_id,
    order_date=order_date,
)
```

**Bad Example #1**: For each person find their best email. This is invalid because `email` is not a sub-collection of `People` (it is a scalar attribute, so there is only 1 `email` per-person).

```py
%%pydough
People(first_name, BEST(email, by=birth_date.DESC()))
```

**Bad Example #2**: For each person find their best package. This is invalid because the `by` argument is missing.

```py
%%pydough
People.BEST(packages)
```

**Bad Example #3**: For each person find their best package. This is invalid because the: `by` argument is not a collation

```py
%%pydough
People.BEST(packages, by=order_date)
```

**Bad Example #4**: For each person find their best package. This is invalid because the `by` argument is empty

```py
%%pydough
People.BEST(packages, by=())
```

**Bad Example #5**: For each person find the 5 most recent packages they have ordered, allowing ties. This is invalid because `n_best` is greater than 1 at the same time that `allow_ties` is True.

```py
%%pydough
People.BEST(packages, by=order_date.DESC(), n_best=5, allow_ties=True)
```

**Bad Example #6**: For each person, find the package cost of their 10 most recent packages. This is invalid because `n_best` is greater than 1, which means that the `BEST` clause is non-singular so its terms cannot be accessed in the calc without aggregating.

```py
%%pydough
best_packages = BEST(packages, by=order_date.DESC(), n_best=10)
People(first_name, best_cost=best_packages.package_cost)
```

**Bad Example #7**: For each person, find the package cost of their most expensive package(s), allowing ties. This is invalid because `allow_ties` is True, which means that the `BEST` clause is non-singular so its terms cannot be accessed in the calc without aggregating.

```py
%%pydough
best_packages = BEST(packages, by=package_cost.DESC(), allow_ties=True)
People(first_name, best_cost=best_packages.package_cost)
```

**Bad Example #8**: For each address, find the package most recently ordered by one of the current occupants of that address, including the address id of the address. This is invalid because `BACK(1)` refers to `current_occupants`, which does not have a field called `address_id`.

```py
%%pydough
most_recent_package = BEST(current_occupants.packages, by=order_date.DESC())
Addresses.most_recent_package(
    address_id=BACK(1).address_id,
    package_id=package_id,
    order_date=order_date,
)
```

**Bad Example #9**: For each address find the oldest occupant. This is invalid because the `BEST` clause is placed in the calc without accessing any of its attributes.

```py
%%pydough
Addresses(address_id, oldest_occupant=BEST(current_occupants, by=birth_date.ASC()))
```

<!-- TOC --><a name="induced-properties"></a>
## Induced Properties

This section of the PyDough specification has not yet been defined.

<!-- TOC --><a name="induced-scalar-properties"></a>
### Induced Scalar Properties

This section of the PyDough specification has not yet been defined.

<!-- TOC --><a name="induced-subcollection-properties"></a>
### Induced Subcollection Properties

This section of the PyDough specification has not yet been defined.

<!-- TOC --><a name="induced-arbitrary-joins"></a>
### Induced Arbitrary Joins

This section of the PyDough specification has not yet been defined.

<!-- TOC --><a name="larger-examples"></a>
## Larger Examples

The rest of the document are examples of questions asked about the data in the people/addresses/packages graph and the corresponding PyDough code, which uses several of the features described in this document.

<!-- TOC --><a name="example-1-highest-residency-density-states"></a>
### Example 1: Highest Residency Density States

**Question**: Find the 5 states with the highest average number of occupants per address.

**Answer**:
```py
%%pydough
# For each address, identify how many current occupants it has
addr_info = Addresses(n_occupants=COUNT(current_occupants))

# Partition the addresses by the state, and for each state calculate the
# average value of `n_occupants` for all addresses in that state
states = PARTITION(
    addr_info,
    name="addrs",
    by=state
)(
    state,
    average_occupants=AVG(addrs.n_occupants)
)

# Obtain the top-5 states with the highest average
result = states.TOP_K(5, by=average_occupants.DESC())
```

<!-- TOC --><a name="example-2-yearly-trans-coastal-shipments"></a>
### Example 2: Yearly Trans-Coastal Shipments

**Question**: For every calendar year, what percentage of all packages are from a customer living in the west coast to an address on the east coast? Only include packages that have already arrived, and order by the year.

**Answer**:
```py
%%pydough
# Contextless expression: identifies if a package comes from the west coast
west_coast_states = ("CA", "OR", "WA", "AK")
from_west_coast = ISIN(customer.current_address.state, west_coast_states)

# Contextless expression: identifies if a pcakge is shipped to the east coast
east_coast_states = ("FL", "GA", "SC", "NC", "VA", "MD", "DE", "NJ", "NY", "CT", "RI", "MA", "NH", "MA")
to_east_coast = ISIN(shipping_address.state, east_coast_states)

# Filter packages to only include ones that have arrived, and derive additional
# terms for if they are trans-coastal + the year they were ordered
package_info = Packages.WHERE(
    PRESENT(arrival_date)
)(
    is_trans_coastal=from_west_coast & to_east_coast,
    year=YEAR(order_date),
)

# Partition the packages by the order year & count how many have a True value
# for is_trans_coastal, vs the total number in that year
year_info = PARTITION(
    package_info,
    name="packs",
    by=year,
)(
    year,
    pct_trans_coastal=100.0 * SUM(packs.is_trans_coastal) / COUNT(packs),
)

# Output the results ordered by year
result = year_info.ORDER_BY(year.ASC())
```

<!-- TOC --><a name="example-3-email-of-oldest-non-customer-resident"></a>
### Example 3: Email of Oldest Non-Customer Resident

**Question**: For every city/state, find the email of the oldest resident of that city/state who has never ordered a package (break ties in favor of the lower social security number). Also include the zip code of that occupant. Order alphabetically by state, followed by city.

**Answer**:
```py
%%pydough

# Partition every address by the city/state
cities = PARTITION(
    Addresses,
    name="addrs",
    by=(city, state)
)

# For each city, find the oldest occupant out of any address in that city
# and include the desired information about that occupant.
oldest_occupants = cities.BEST(
    addrs.current_occupants.WHERE(HASNOT(packages)),
    by=(birth_date.ASC(), ssn.ASC()),
)(
    state=BACK(2).state,
    city=BACK(2).city,
    email=email,
    zip_code=BACK(1).zip_code,
)

# Sort the output by state, followed by city
result = oldest_occupants.ORDER_BY(
    state.ASC(),
    city.ASC(),
)
```

<!-- TOC --><a name="example-4-outlier-packages-per-month-of-2017"></a>
### Example 4: Outlier Packages Per Month Of 2017

**Question**: For every month of the year 2017, identify the percentage of packages ordered in that month that are at least 10x the average value of all packages ordered in 2017. Order the results by month.

**Answer**:
```py
%%pydough
# Contextless expression: identifies is a package was ordered in 2017
is_2017 = YEAR(order_date) == 2017

# Identify the average package cost of all packages ordered in 2017
global_info = GRAPH(
    avg_package_cost=AVG(Packages.WHERE(is_2017).package_cost)
)

# Identify all packages ordered in 2017, but where BACK(1) is global_info
# instead of GRAPH, so we have access to global_info's terms.
selected_package = global_info.Packages.WHERE(is_2017)

# For each such package, identify the month it was ordered, and add a term to
# indicate if the cost of the package is at least 10x the average for all such
# packages.
packages = selected_packages(
    month=MONTH(order_date),
    is_10x_avg=package_cost >= (10.0 * BACK(1).avg_package_cost)
)

# Partition the packages by the month they were ordered, and for each month
# calculate the ratio between the number of packages where is_10x_avg is True
# versus all packages ordered that month, multiplied by 100 to get a percentage.
months = PARTITION(
    package_info,
    name="packs",
    by=month
)(
    month,
    pct_outliers=100.0 * SUM(packs.is_10x_avg) / COUNT(packs)
)

# Order the output by month
result = months.ORDER_BY(month.ASC())
```

<!-- TOC --><a name="example-5-regression-prediction-of-packages-quantity"></a>
### Example 5: Regression Prediction Of Packages Quantity

**Question**: Using linear regression of the number of packages ordered per-year, what is the predicted number of packages for the next three years?

Note: uses the formula [discussed here](https://medium.com/swlh/linear-regression-in-sql-is-it-possible-b9cc787d622f) to identify the slope via linear regression.

**Answer**:
```py
%%pydough
# Identify every year & how many packages were ordered that year
yearly_data = PARTITION(
    Packages(year=YEAR(order_date)),
    name="packs",
    by=year,
)(
    year,
    n_orders = COUNT(packs),
)

# Obtain the global average of the year (x-coordinate) and
# n_orders (y-coordinate). These correspond to `x-bar` and `y-bar`.
global_info = GRAPH(
    avg_x = AVG(yearly_data.year),
    avg_y = AVG(yearly_data.n_orders),
)

# Contextless expression: corresponds to `x - x-bar` with regards to yearly_data
# inside of global_info
dx = n_orders - BACK(1).avg_x

# Contextless expression: corresponds to `y - y-bar` with regards to yearly_data
# inside of global_info
dy = year - BACK(1).avg_y

# Contextless expression: derive the slope with regards to global_info
regression_data = yearly_data(value=(dx * dy) / (dx * dx))
slope = SUM(regression_data.value)

# Identify the (chronologically) last record from yearly_data.
# Could also write as `last_year = packs.WHERE(RANKING(by=year.DESC()) == 1).SINGULAR()`
last_year = BEST(packs, by=year.DESC())

# Use a loop to derive a pair of terms for each of the 3 next years:
# 1. The year itself
# 2. The predicted number of orders (should be the last year's orders + slope * number of years)
# This is allowed since calcs can operate via keyword arguments, whether real
# or passed in via a dictionary with ** syntax.
results = {}
for n in range(1, 4):
    results[f"year_{n}"] = last_year.year + n
    results[f"year_{n}_prediction"] = last_year.n_orders + (n * slope)
result = global_info(**results)
```
