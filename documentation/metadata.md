# PyDough Metadata Spec

This page document the exact format that the JSON files containing PyDough metadata must ascribe to. The specification of this format is for the initial release of PyDough, but is intended to change drastically in the near future.

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [JSON File Structure](#json-file-structure)
- [Collections](#collections)
   * [Collection Type: Simple Table](#collection-type-simple-table)
- [Properties](#properties)
   * [Property Type: Table Column](#property-type-table-column)
- [Relationship](#relationship)
   * [Relationship Type: Simple Join](#relationship-type-simple-join)
   * [Relationship Type: General Join](#relationship-type-general-join)
   * [Relationship Type: Cartesian Product](#relationship-type-cartesian-product)
- [PyDough Type Strings](#pydough-type-strings)
- [Metadata Samples](#metadata-samples)
   * [Example: TPC-H](#example-tpch)

<!-- TOC end -->

<!-- TOC --><a name="json-file-structure"></a>
## JSON File Structure

The core components of the JSON file structure are as follows:

- Each JSON file for PyDough metadata is a JSON array where each item is a JSON object representing a single knowledge graph.
- Each knowledge graph object must have a `name` and `version` property, whose values are strings. The `name` property must be a valid Python identifier, and the `version` property can currently only be `V2` (indicating that the `V2` version of metadata should be used, which is the only current version of metadata).
- Specification for the rest of the fields of the graph object if the version is `V2`:
  - `collections` (required): an array of the metadata for each collection in the graph ([see here for more details](#collections))
  - `relationships` (required): an array of the metadata for each relationship in the graph ([see here for more details](#relationships))
  - `additional definitions` (optional): an array of strings where each string is a sentence defining a concept/definition within the semantical context of the graph, such as the vocabulary for a specific kind of analysis and how to compute it in terms of the collections/relationships.
  - `verified pydough analysis` (optional): an array of JSON objects where each object represents an instance of a question & answer pair with PyDough using the data from the graph. The object has two fields that are both strings: `question` maps to the question being asked, and `code` is the PyDough code that solves the question using hte graph (can be multiline).
  - `extra semantic info` (optional): an object containing arbitrary additional semantic information about the entire graph (internal use only).
- Example of the structure of a metadata JSON file containing two graphs, both V2
```json
[
  {
    "name": "BankerGraph",
    "version": "V2",
    "collections": [...],
    "relationships": [...],
    "additional definitions": [...]
    "verified pydough analysis": [
      {"question": ..., "code": ...},
      ...
    ],
    "extra semantic info": {...}
  },
  {
    "name": "GroceryGraph",
    "version": "V2",
    "collections": [...],
    "relationships": [...]
  }
]
```

<!-- TOC --><a name="collections"></a>
## Collections

Every JSON object describing a PyDough collection has the following fields:
- `name` (required): the name of the collection, which must be a valid PyDough identifier and cannot be the same as the name of the graph.
- `type` (required): the type of PyDough collection (currently only supports `"simple table"`).
- `description` (optional): a semantic description of the collection's significance.
- `synonyms` (optional): a list of strings of alternative names for the collection, for semantic understanding.

<!-- TOC --><a name="collection-type-simple-table"></a>
### Collection Type: Simple Table

A collection with this type is essentially just a view of a table stored in whatever database is being used to execute the queries. Collections of this type have a type string of "simple table" and the following additional key-value pairs in their metadata JSON object:

- `table path` (required): a string indicating the fully-qualified path to reach the table based on where it is stored, e.g. `<db_name>.<schema_name>.<table_name>`. The table_name here does not need to be the same as the name of the collection.
- `unique properties` (required): a list of JSON values indicating which properties are unique within the collection, meaning that no two rows of the table will have the same values. If a value in this list is a string, it means that property is unique within the collection. If a value in the list is a list of strings, it means that every combination of the properties in that list is unique within the collection.
  - Example A: `"unique properties": ["k1"]` means every value of property `k1` is unique.
  - Example B: `"unique properties": ["j1", "j2"]` means every value of property `j1` is unique, and every value of property `j2` is unique.
  - Example C: `"unique properties": [["q1", "q2"]]` means every combination of values of properties `q1` and `q2` is unique.
  - Example D: `"unique properties": ["r1", ["r2", "r3", "r4"]]` means every value of property `r1` is unique, and every combination of values of properties `r2`, `r3` and `r4` is unique.
- `properties` (required): an array of objects representing scalar properties of each record from the collection ([see here for details](#properties)).

Example of the structure of the metadata for a simple table collection:

```json
{
    "name": "accounts",
    "type": "simple table",
    "table_path": "bank_schema.ACCOUNTS",
    "unique_properties": ["account_id"],
    "properties": {
        "account_id": {...},
        "client_id": {...},
        "account_type": {...},
        "account_balance": {...},
        "date_opened": {...}
    },
    "description": "Every account currently open in the bank's system",
    "extra semantic info": {...}
}
```

<!-- TOC --><a name="properties"></a>
## Properties

Every JSON object describing a scalar property of a PyDough collection ahs the following fields:
- `name` (required): the name of the property, which must be a valid PyDough has and must be unique within the collection (and cannot conflict with the name of a relationship that the colleciton has)
- `type` (required): the type of PyDough collection property (currently only supports `"table column"`)
- `description` (optional): a semantic description of the property's significance.
- `sample values` (optional): an array of JSON objects where each value is an example of a value that can be found in the actual data for tha property.
- `synonyms` (optional): a list of strings of alternative names for the property, for semantic understanding.

<!-- TOC --><a name="property-type-table-column"></a>
### Property Type: Table Column

A property with this type is essentially just a view of a column in a table stored in whatever database is being used to execute the queries. Properties of this type are scalar expression attributes of the collection, as opposed to sub-collections. Properties of this type have a type string of "table column" and have the following additional key-value pairs in their metadata JSON object:

- `column name` (required): a string indicating the name of the column within the table it comes from, which it can be different from the name of the property itself.
- `data type` (required): a string indicating the PyDough type that this column corresponds to ([see here for details](#pydough-type-strings)).

Example of the structure of the metadata for a table column property:

```json
{
    "name": "account balance",
    "type": "table column",
    "column_name": "ba_bal",
    "data_type": "numeric",
    "description": "The amount of money currently in the account",
    "sample values": [0.0, 123.45, 999864.00],
    "synonyms": ["amount", "value", "balance"],
    "extra semantic info": {...}
}
```

<!-- TOC --><a name="relationships"></a>
## Relationships

Every JSON object describing a relationship between two PyDough collection has the following fields:
- `name` (required): the name of the relationshp, which must be a valid PyDough identifier and must not overlap with the name of other properties/relationships from the source collection.
- `type` (required): the type of PyDough relationship. The currently supported values are ["simple join"](#relationship-type-simple-join), ["general join"](#relationship-type-general-join), ["cartesian product"](#relationship-type-cartesian-product), ["custom"](#relationship-type-custom) and ["reverse"](#relationship-type-reverse)
- `description` (optional): a semantic description of the relationship's significance.
- `synonyms` (optional): a list of strings of alternative names for the relationship, for semantic understanding.

<!-- TOC --><a name="relationship-type-simple-join"></a>
### Relationship Type: Simple Join

A property with this type describes a subcollection of the current collection that is derived from performing an equi-join on two tables (e.g. `SELECT ... FROM T1 JOIN T2 ON T1.a=T2.x AND T1.b = T2.y`). Relationships of this type are sub-collections of the collection, as opposed to scalar attributes. Relationships of this type have a type string of "simple join" and have the following additional key-value pairs in their metadata JSON object:

- `parent collection` (required): a string indicating the name of the parent collection that the relationship connects from. This must be the name of one of the collections in the graph.
- `child collection` (required): a string indicating the name of the child collection that the the relationship maps to. This must be the name of one of the collections in the graph.
- `singular` (required): a boolean that is true if each record in the parent collection has at most 1 matching record of the child, and false otherwise.
- `always matches` (optional): a boolean that is true if every record from the parent collection matches onto at least 1 record from the child, and false otherwise (default: `false`).
- `keys` (required): a JSON object indicating the combinations of properties from the parent collection and the child collection that are compared for equality in order to determine join matches. The keys to this JSON object are the names of properties in the current collection, and the values are a list of 1+ strings that are the names of properties in the other collection that they must be equal to in order to produce a match. All property names invoked in the keys object must correspond to scalar attributes of the collections. This object must be non-empty, and all value lists must also be non-empty.

Example of the structure of the metadata for a simple join property (connects a collection `Clients` to a collection `Accounts` by joining `Clients.id` on `Accounts.client_id`):

```json
{
    "name": "accounts_held",
    "type": "simple join",
    "parent collection": "Clients",
    "child collection": "Accounts",
    "singular": false,
    "always matches": false,
    "keys": {"id": ["client_id"]},
    "description": "All of the accounts currently held by the client in the bank's system",
    "synonyms": ["current accounts", "open accounts"]
}
```

<!-- TOC --><a name="relationship-type-general-join"></a>
### Relationship Type: General Join

This relationship type is a variant of the simple join, which instead of joining on equality of key columns has an arbitrary PyDough expression as a condition. This is useful for more arbitrary join conditions such as interval joins (e.g. `SELECT ... FROM T1 JOIN T2 ON T1.a <= T2.B AND T2.B <= T1.c`). Relationships of this type are sub-collections of the collection, as opposed to scalar attributes. Relationships of this type have a type string of "general_join" and have the following additional key-value pairs in their metadata JSON object:

- `parent collection` (required): a string indicating the name of the parent collection that the relationship connects from. This must be the name of one of the collections in the graph.
- `child collection` (required): a string indicating the name of the child collection that the the relationship maps to. This must be the name of one of the collections in the graph.
- `singular` (required): a boolean that is true if each record in the parent collection has at most 1 matching record of the child, and false otherwise.
- `always matches` (optional): a boolean that is true if every record from the parent collection matches onto at least 1 record from the child, and false otherwise (default: `false`).
- `condition` (required): a string representing the PyDough code for the condition, where columns from the current collection are referred to as `self.xyz` and columns from the child collection being accessed are referred to as `other.xyz`. This can be any valid PyDough code, with the following caveats:
  - The code must be a single Python expression.
  - The code must return a PyDough expression, as opposed to a collection.
  - The code may call regular PyDough functions, but not window functions.
  - The code may not access any sub-collections of either collection.

Example of the structure of the metadata for a simple join property (connects a collection `Incidents` to a collection `WorkerShifts` by joining on whether `Incidents.incident_timestamp` is between `WorkerShifts.start_of_shift` and `WorkerShifts.end_of_shift`):

```json
{
    "name": "shifts_overlapping_with_incident",
    "type": "general join",
    "parent collection": "Incidents",
    "child collection": "WorkerShifts",
    "singular": false,
    "always matches": false,
    "condition": "MONOTONIC(other.start_of_shift, self.incident_timestamp, other.end_of_shift)",
}
```

<!-- TOC --><a name="relationship-type-cartesian-product"></a>
### Relationship Type: Cartesian Product

A relationship with this type describes a subcollection of the current collection that is derived from performing an cross-join on two collections (e.g. SELECT ... FROM T1, T2). Relationships of this type are sub-collections of the collection, as opposed to scalar attributes. Relationships of this type have a type string of "cartesian product" and have the following additional key-value pairs in their metadata JSON object:

- `parent collection` (required): a string indicating the name of the parent collection that the relationship connects from. This must be the name of one of the collections in the graph.
- `child collection` (required): a string indicating the name of the child collection that the the relationship maps to. This must be the name of one of the collections in the graph.
- `always matches` (optional): a boolean that is true if every record from the parent collection matches onto at least 1 record from the child, and false otherwise (default: `true`). This should always be true unless it is possible for the child collection to be empty.

Example of the structure of the metadata for a cartesian product property (connects every record of a collection `CalendarDates` to every record of collection `InventorySnapshots`):

```json
{
  "name": "snapshots",
  "type": "cartesian_product",
  "parent collection": "CalendarDates",
  "child collection": "InventorySnapshots",
  "always matches": true,
  "description": "Every single snapshot, accessed from every single calendar date",
  "synonyms": ["inventory logs", "storage records", "manifests"]
}
```

<!-- TOC --><a name="relationship-type-custom"></a>
### Relationship Type: Custom

> [!IMPORTANT]
> This type of metadata relationship has not yet been implemented.

<!-- TOC --><a name="relationship-type-reverse"></a>
### Relationship Type: Reverse

A relationship with this type specifies that a relationship should be created by flipping the direction of an existing relationship that was already defined earlier in the `relationships` array. Relationships of this type are sub-collections of the collection, as opposed to scalar attributes. Relationships of this type have a type string of "cartesian product" and have the following additional key-value pairs in their metadata JSON object:
- `original parent` (required): a string indicating the name of the collection who holds the relationship being reversed (this will be the child of hte new property)
- `original property` (required): a string indicating the name of the relationship property of the parent that is being reversed.
- `singular` (optional): a boolean that is true if every record from the parent collection (the original child collection) matches onto at least 1 record from the child (the original parent), and false otherwise (default: `true`). This should always be true unless it is possible for the child collection to be empty.
- `always matches` (optional): a boolean that is true if every record from the parent collection matches onto at least 1 record from the child, and false otherwise (default: `false`).


Example of the structure of the metadata for a reverse (flips the earlier defined `Clients.accounts_held` relationship, connecting each record of `Accounts` to the client who holds it):

```json
{
  "name": "client",
  "type": "reverse",
  "original parent": "Clients",
  "original property": "accounts_held",
  "singular": true,
  "always matches": true,
  "description": "The client who holds the account",
  "synonyms": ["account holder", "owner"]
}
```


<!-- TOC --><a name="pydough-type-strings"></a>
## PyDough Type Strings

The strings used in the type field for certain properties must be one of the following:

- `numeric`: any numerical data such as integers, floats, decimals, regardless of scale/precision.
- `bool`: a boolean.
- `string`: a string or other bytes-like format (char, varchar, binary, varbinary, etc.).
- `datetime`: any date/timestamp type, regardless of precision or timezone.
- `array[t]`: an array of values of type t (where t is another PyDough type). For example: `array[int32]` or `array[array[string]]`.
- `map[t1,t2]`: a map of values with keys of type type t1 and values of type t2 (where t1 and t2 are also PyDough types). For example: `map[string,numeric]` or `map[string,array[date]]`.
- `struct[field1:t1,field2:t2,...]`: a struct of values with fields named field1, field2, etc. with types t1, t2, etc. (which are also PyDough types). For example: `struct[x:int32,y:int32]` or `struct[name:string,birthday:datetime,car_accidents:array[struct[ts:timestamp[9],report:string]]`. Each field name must be a valid Python identifier.
- `unknown`: an unknown/other type.

<!-- TOC --><a name="metadata-samples"></a>
## Metadata Samples


<!-- TOC --><a name="example-tpch"></a>
### Example: TPC-H

This example of a PYDough metadata JSON contains a single knowledge graph for the TPC-H database ([see here for spec details](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf)).

```json
[
  {
    "name": "TPCH",
    "version": "V2",
    "collections": [
      {
        "name": "regions",
        "type": "simple table",
        "table path": "tpch.REGION",
        "unique properties": ["key"],
        "properties": [
          {
            "name": "key",
            "type": "table column",
            "column name": "r_regionkey",
            "data type": "numeric",
            "description": "Unique identifier id for the region",
            "sample values": [0, 1, 2, 3, 4],
            "synonyms": ["id"]
          },
          {
            "name": "name",
            "type": "table column",
            "column name": "r_name",
            "data type": "string",
            "description": "Uppercase name of the region",
            "sample values": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]
          },
          {
            "name": "comment",
            "type": "table column",
            "column name": "r_comment",
            "data type": "string",
            "description": "Comment/remark on the region"
          }
        ],
        "description": "The regions of the world",
        "synonyms": ["continents", "segments of the world"]
      },
      {
        "name": "nations",
        "type": "simple table",
        "table path": "tpch.NATION",
        "unique properties": ["key"],
        "properties": [
          {
            "name": "key",
            "type": "table column",
            "column name": "n_nationkey",
            "data type": "numeric",
            "description": "Unique identifier id for the nation",
            "sample values": [0, 1, 10, 13, 24],
            "synonyms": ["id"]
          },
          {
            "name": "region_key",
            "type": "table column",
            "column name": "n_regionkey",
            "data type": "numeric",
            "description": "Key from the region that the nation belongs to",
            "sample values": [0, 1, 2, 3, 4]
          },
          {
            "name": "name",
            "type": "table column",
            "column name": "n_name",
            "data type": "string",
            "description": "Uppercase name of the nation",
            "sample values": ["KENYA", "PERU", "JAPAN", "INDIA", "GERMANY"]
          },
          {
            "name": "comment",
            "type": "table column",
            "column name": "n_comment",
            "data type": "string",
            "description": "Comment/remark on the nation"
          }
        ],
        "description": "The nations of the world",
        "synonyms": ["countries", "states"]
      },
      {
        "name": "parts",
        "type": "simple table",
        "table path": "tpch.PART",
        "unique properties": ["key"],
        "properties": [
          {
            "name": "key",
            "type": "table column",
            "column name": "p_partkey",
            "data type": "numeric",
            "description": "Unique identifier id for the part",
            "sample values": [0, 103719, 114994, 64760, 2440],
            "synonyms": ["id"]
          },
          {
            "name": "name",
            "type": "table column",
            "column name": "p_name",
            "data type": "string",
            "description": "Name of the part, consisting of multiple lowercase colors",
            "sample values": ["chiffon plum white linen firebrick", "chocolate steel antique green lavender", "sky frosted cream light blush"],
            "synonyms": ["colors", "description"]
          },
          {
            "name": "manufacturer",
            "type": "table column",
            "column name": "p_mfgr",
            "data type": "string",
            "description": "Name of the manufacturer of the part (not the same as the suppliers). The manufacturer number is the same as the first digit of the part's brand number.",
            "sample values": ["Manufacturer#1", "Manufacturer#2", "Manufacturer#3", "Manufacturer#4", "Manufacturer#5"]
          },
          {
            "name": "brand",
            "type": "table column",
            "column name": "p_brand",
            "data type": "string",
            "description": "The production brand that the part belongs to. The brand contains two digits where the first digit is the same as the manufacturer number (e.g. Brand#32 is from Manufacturer #3).",
            "sample values": ["Brand#11", "Brand#23", "Brand#34", "Brand#45", "Brand#55"]
          },
          {
            "name": "part_type",
            "type": "table column",
            "column name": "p_type",
            "data type": "string",
            "description": "The type of the part, consisting of three uppercase descriptors where the first is a category (e.g. 'STANDARD' or 'PROMO'), the second is a processing state (e.g. 'ANODIZED' or 'PLATED') and the third is a material (e.g. 'STEEL' or 'BRASS').",
            "sample values": ["ECONOMY ANODIZED NICKEL", "PROMO BURNISHED COPPER", "STANDARD BRUSHED STEEL", "SMALL PLATED TIN", "LARGE POLISHED BRASS", "MEDIUM PLATED NICKEL"],
            "synonyms": ["category", "descriptor", "processing", "material"]
          },
          {
            "name": "size",
            "type": "table column",
            "column name": "p_size",
            "data type": "numeric",
            "description": "The size of the part",
            "sample values": [1, 10, 31, 46, 50],
            "synonyms": ["dimension", "measurement", "length", "width", "height", "volume"]
          },
          {
            "name": "container",
            "type": "table column",
            "column name": "p_container",
            "data type": "string",
            "description": "The container that the part is stored in. The container consists of two uppercase descriptors where the first is a size (e.g. 'SM' or 'JUMBO') and the second is a type of container (e.g. 'BOX' or 'JAR').",
            "sample values": ["SM CASE", "LG BOX", "MED BAG", "JUMBO JAR", "WRAP PKG", "SM PACK", "LG CAN", "MED DRUM"],
            "synonyms": ["vessel", "packaging", "receptacle"]
          },
          {
            "name": "retail_price",
            "type": "table column",
            "column name": "p_retailprice",
            "data type": "numeric",
            "description": "The retail price of the part, which it is intended to be sold for before accounting for the price the supplier charges, in US dollars. The price is rounded to the nearest cent, and most of the values are between $900 and $2000.",
            "sample values": [901.00, 2098.99, 14499.50, 2080.99, 2050.96, 1476.41],
            "synonyms": ["listed selling price", "wholesale value"]
          },
          {
            "name": "comment",
            "type": "table column",
            "column name": "p_comment",
            "data type": "string",
            "description": "Description/commentary on the part"
          }
        ],
        "description": "The various products supplied by various companies in shipments to different customers",
        "synonyms": ["products", "components", "items", "goods"]
      },
      {
        "name": "suppliers",
        "type": "simple table",
        "table path": "tpch.SUPPLIER",
        "unique properties": ["key", "name"],
        "properties": [
          {
            "name": "key",
            "type": "table column",
            "column name": "s_suppkey",
            "data type": "numeric",
            "description": "Unique identifier id for the supplier",
            "sample values": [2452, 8063, 1, 10000, 5053],
            "synonyms": ["id"]
          },
          {
            "name": "name",
            "type": "table column",
            "column name": "s_name",
            "data type": "string",
            "description": "Name of the supplier, which is always Supplier#<number> where the number is the same as the supplier's key, prepended with zeros until it is 9 digits",
            "sample values": ["Supplier#000008427", "Supplier#000001917", "Supplier#000000001", "Supplier#000010000", "Supplier#000000893"]
          },
          {
            "name": "address",
            "type": "table column",
            "column name": "s_address",
            "data type": "string",
            "description": "Address of the supplier as a cryptographically encrypted string to anonymize the data.",
            "sample values": ["aSYD1SvrdIGV8LxRL QDp5m9dV", "ydl44utgudl6CP46TF7kliIcF5sC8K9,WH,Tj", "J1Vd3lqn1UvN2|4|14-632-452-6847"],
            "synonyms": ["location", "street address", "corporate address", "headquarters"]
          },
          {
            "name": "nation_key",
            "type": "table column",
            "column name": "s_nationkey",
            "data type": "numeric",
            "description": "Key from the nation that the supplier belongs to",
            "sample values": [0, 1, 10, 13, 24],
            "synonyms": ["nation id"]
          },
          {
            "name": "phone",
            "type": "table column",
            "column name": "s_phone",
            "data type": "string",
            "description": "Phone number of the supplier in the format 'CC-XXX-XXX-XXXX' where CC is the country code (each nation has a unique country code).",
            "sample values": ["25-995-176-6622", "18-132-649-2520", "30-505-249-4504", "10-132-649-2520", "27-599-541-3605"],
            "synonyms": ["contact number", "telephone number"]
          },
          {
            "name": "account_balance",
            "type": "table column",
            "column name": "s_acctbal",
            "data type": "numeric",
            "description": "The account balance of the supplier in US dollars. The balance is rounded to the nearest cent and most of the values are between -$1,000 and +$10,000.",
            "sample values": [-998.22, 9999.72, 4510.35, 9125.21, -0.92, 58.93],
            "synonyms": ["balance", "credit", "wealth", "debt", "surplus", "cash on hand", "money in bank"]
          },
          {
            "name": "comment",
            "type": "table column",
            "column name": "s_comment",
            "data type": "string",
            "description": "Commentary/remark on the supplier"
          }
        ],
        "description": "The various companies that supply different parts to fulfill purchase orders",
        "synonyms": ["companies", "businesses", "vendors"]
      },
      {
        "name": "lines",
        "type": "simple table",
        "table path": "tpch.LINEITEM",
        "unique properties": [["order_key", "line_number"]],
        "properties": [
          {
            "name": "order_key",
            "type": "table column",
            "column name": "l_orderkey",
            "data type": "numeric",
            "description": "Key from the order that the line item belongs to",
            "sample values": [5294597, 19010, 68581, 2710114, 2462791],
            "synonyms": ["order id"]
          },
          {
            "name": "part_key",
            "type": "table column",
            "column name": "l_partkey",
            "data type": "numeric",
            "description": "Key from the part that the lineitem describes a purchase/shipment of",
            "sample values": [1, 103719, 114994, 64760, 2440],
            "synonyms": ["part id"]
          },
          {
            "name": "supplier_key",
            "type": "table column",
            "column name": "l_suppkey",
            "data type": "numeric",
            "description": "Key from the supplier that the lineitem describes a purchase/shipment from",
            "sample values": [2452, 8063, 1, 10000, 5053],
            "synonyms": ["supplier id"]
          },
          {
            "name": "line_number",
            "type": "table column",
            "column name": "l_linenumber",
            "data type": "numeric",
            "description": "The line number of the lineitem within the order. Each lineitem within an order has its own line number, and represents a purchase of a part from a supplier within the order.",
            "sample values": [1, 2, 3, 4, 5, 6, 7],
            "synonyms": ["line id", "shipment index within order"]
          },
          {
            "name": "quantity",
            "type": "table column",
            "column name": "l_quantity",
            "data type": "numeric",
            "description": "The number of units of the part that is being purchased in the lineitem, as a number between 1 and 50",
            "sample values": [1, 10, 13, 25, 48, 50],
            "synonyms": ["amount", "purchase volume", "units", "count", "number of items", "shipment size"]
          },
          {
            "name": "extended_price",
            "type": "table column",
            "column name": "l_extendedprice",
            "data type": "numeric",
            "description": "The extended price of the line item, which is the retail price of the part multiplied by the quantity purchased (before any discounts/taxes are applied). The price is rounded to the nearest cent and most of the values are between $900 and $100,000.",
            "sample values": [901.00, 36036.00, 57657.60, 50450.4, 39097.8],
            "synonyms": ["raw price", "gross cost", "total value before discount/tax"]
          },
          {
            "name": "discount",
            "type": "table column",
            "column name": "l_discount",
            "data type": "numeric",
            "description": "The discount applied to the line item, which is a ratio between 0 and 1 representing percentage of the extended price. The percentage is always between 0% (0.00) and 10% (0.10)",
            "sample values": [0.00, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10],
            "synonyms": ["markdown", "price reduction"]
          },
          {
            "name": "tax",
            "type": "table column",
            "column name": "l_tax",
            "data type": "numeric",
            "description": "The sales tax applied to the line item, which is a ratio between 0 and 1 representing percentage of the extended price. The percentage is always between 0% (0.00) and 8% (0.08)",
            "sample values": [0.00, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08],
            "synonyms": ["levy", "duty", "tariff"]
          },
          {
            "name": "status",
            "type": "table column",
            "column name": "l_linestatus",
            "data type": "string",
            "description": "The status of the line item, which is always 'O' (for orders that have been placed but not yet filled, e.g. pending shipment) or 'F' (for orders that have been filled, e.g. already shipped)",
            "sample values": ["O", "F"],
            "synonyms": ["pending shipment", "shipment state", "fulfilled"]
          },
          {
            "name": "ship_date",
            "type": "table column",
            "column name": "l_shipdate",
            "data type": "datetime",
            "description": "The date that the line item was shipped from the supplier/warehouse. The date is always between 1992-01-01 and 1998-12-31"
          },
          {
            "name": "commit_date",
            "type": "table column",
            "column name": "l_commitdate",
            "data type": "datetime",
            "description": "The date that the line item was committed to be shipped from the supplier/warehouse. The date is always between 1992-01-01 and 1998-12-31. The ship date is ideally before or on the actual commit date, but can be after it if the shipment was delayed."
          },
          {
            "name": "receipt_date",
            "type": "table column",
            "column name": "l_receiptdate",
            "data type": "datetime",
            "description": "The date that the line item was received by the customer. The date is always between 1992-01-01 and 1998-12-31. The receipt date is after the ship date due to the time to ship the package."
          },
          {
            "name": "ship_instruct",
            "type": "table column",
            "column name": "l_shipinstruct",
            "data type": "string",
            "description": "The shipping instructions for the line item, which is always 'DELIVER IN PERSON', 'TAKE BACK RETURN', 'COLLECT COD' or 'NONE'",
            "sample values": ["DELIVER IN PERSON", "TAKE BACK RETURN", "COLLECT COD", "NONE"],
            "synonyms": ["shipping instructions", "delivery instructions"]
          },
          {
            "name": "ship_mode",
            "type": "table column",
            "column name": "l_shipmode",
            "data type": "string",
            "description": "The shipping mode for the line item, which is always 'AIR', 'AIR REG', 'FOB', 'MAIL' or 'SHIP'",
            "sample values": ["AIR", "AIR REG", "FOB", "MAIL", "SHIP"],
            "synonyms": ["shipping method", "delivery method"]
          },
          {
            "name": "return_flag",
            "type": "table column",
            "column name": "l_returnflag",
            "data type": "string",
            "description": "The return flag for the line item, which is always 'R' (for returned items) or 'N' (for non-returned items)",
            "sample values": ["R", "N"],
            "synonyms": ["return status"]
          },
          {
            "name": "comment",
            "type": "table column",
            "column name": "l_comment",
            "data type": "string",
            "description": "Commentary/remark on the line item"
          }
        ],
        "description": "The line items for shipments within an order. Each line item within an order has its own line number, and represents a purchase of a part from a supplier within the order. The order can contain multiple lineitems from different suppliers for different parts, and the lineitems can each have their own shipping information.",
        "synonyms": ["shipments", "packages", "purchases", "deliveries", "order components", "order elements"]
      },
      {
        "name": "supply_records",
        "type": "simple table",
        "table path": "tpch.PARTSUPP",
        "unique properties": [["part_key", "supplier_key"]],
        "properties": [
          {
            "name": "part_key",
            "type": "table column",
            "column name": "ps_partkey",
            "data type": "numeric",
            "description": "Key from the part that the supply record belongs to",
            "sample values": [1, 103719, 114994, 64760, 2440],
            "synonyms": ["part id"]
          },
          {
            "name": "supplier_key",
            "type": "table column",
            "column name": "ps_suppkey",
            "data type": "numeric",
            "description": "Key from the supplier that the supply record belongs to",
            "sample values": [2452, 8063, 1, 10000, 5053],
            "synonyms": ["supplier id"]
          },
          {
            "name": "available_quantity",
            "type": "table column",
            "column name": "ps_availqty",
            "data type": "numeric",
            "description": "The number of units of the part that the supplier has available to supply. The quantity is usually between 1 and 1,000",
            "sample values": [4620, 3100, 6547, 3337, 76],
            "synonyms": ["available stock", "inventory", "supply", "quantity remaining"]
          },
          {
            "name": "supply_cost",
            "type": "table column",
            "column name": "ps_supplycost",
            "data type": "numeric",
            "description": "The cost that it takes for the supplier to produce a single unit of the part. The cost is rounded to the nearest cent and most of the values are between $1 and $1,000.",
            "sample values": [144.43, 772.21, 285.90, 50.12, 983.998],
            "synonyms": ["production cost", "manufacturing cost", "cost of goods sold"]
          },
          {
            "name": "comment",
            "type": "table column",
            "column name": "ps_comment",
            "data type": "string",
            "description": "Commentary/remark on the supply record"
          }
        ],
        "description": "Every combination of a supplier and a part that the supplier supplies. Each record contains information about the supplier of the part, the part itself, and the availability of the part from the supplier.",
        "synonyms": ["supplier part information", "partsupp info", "manifest", "inventories", "catalog"]
      },
      {
        "name": "orders",
        "type": "simple table",
        "table path": "tpch.ORDERS",
        "unique properties": ["key"],
        "properties": [
          {
            "name": "key",
            "type": "table column",
            "column name": "o_orderkey",
            "data type": "numeric",
            "description": "Unique identifier id for the order",
            "sample values": [317728, 1096707, 5522855, 2624837, 1866566],
            "synonyms": ["id"]
          },
          {
            "name": "customer_key",
            "type": "table column",
            "column name": "o_custkey",
            "data type": "numeric",
            "description": "Key from the customer that placed the order",
            "sample values": [93721, 65251, 81379, 20663, 42247],
            "synonyms": ["customer id"]
          },
          {
            "name": "order_status",
            "type": "table column",
            "column name": "o_orderstatus",
            "data type": "string",
            "description": "The status of the order, which is always 'O' for orders where all line items of the order have status 'O', 'F' for orders where all line items of the order have status 'F', and 'P' otherwise",
            "sample values": ["O", "F", "P"],
            "synonyms": ["order state", "fulfillment status"]
          },
          {
            "name": "total_price",
            "type": "table column",
            "column name": "o_totalprice",
            "data type": "numeric",
            "description": "The total price of the order after any discounts/taxes are applied, which is the sum of the extended price * (1 - discount) * (1 - tax) for all line items in the order. The price is rounded to the nearest cent and most of the values are between $800 and $600,000.",
            "sample values": [857.71, 555285.16, 3618.2, 277554.58, 52737.18],
            "synonyms": ["total cost", "total value"]
          },
          {
            "name": "order_date",
            "type": "table column",
            "column name": "o_orderdate",
            "data type": "datetime",
            "description": "The date that the order was placed. The date is always between 1992-01-01 and 1998-12-31",
            "synonyms": ["order placed date", "order creation date", "purchase date"]
          },
          {
            "name": "order_priority",
            "type": "table column",
            "column name": "o_orderpriority",
            "data type": "string",
            "description": "The priority of the order, which is always '1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED' or '5-LOW'",
            "sample values": ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"],
            "synonyms": ["urgency", "priority level"]
          },
          {
            "name": "clerk",
            "type": "table column",
            "column name": "o_clerk",
            "data type": "string",
            "description": "The clerk that processed the order, which is always 'Clerk#<number>' where the number is prepended with zeros until it is 9 digits",
            "sample values": ["Clerk#000000001", "Clerk#000000090", "Clerk#000000635", "Clerk#000000892", "Clerk#000000073"],
            "synonyms": ["salesperson", "representative", "agent", "notary", "officiant", "registrar", "overseer"]
          },
          {
            "name": "ship_priority",
            "type": "table column",
            "column name": "o_shippriority",
            "data type": "numeric",
            "description": "The priority of the order for shipping, which is always 0",
            "sample values": [0],
            "synonyms": ["shipping priority", "shipment urgency"]
          },
          {
            "name": "comment",
            "type": "table column",
            "column name": "o_comment",
            "data type": "string",
            "description": "Commentary/remark on the order"
          }
        ],
        "description": "The orders that have been placed by customers. Each order can contain multiple lineitems from different suppliers for different parts, and the lineitems can each have their own shipping information.",
        "synonyms": ["transactions"]
      },
      {
        "name": "customers",
        "type": "simple table",
        "table path": "tpch.CUSTOMER",
        "unique properties": ["key", "name"],
        "properties": [
          {
            "name": "key",
            "type": "table column",
            "column name": "c_custkey",
            "data type": "numeric",
            "description": "Unique identifier id for the customer",
            "sample values": [93721, 65251, 81379, 20663, 42247],
            "synonyms": ["id"]
          },
          {
            "name": "name",
            "type": "table column",
            "column name": "c_name",
            "data type": "string",
            "description": "Name of the customer, which is always Customer#<number> where the number is the same as the customer's key, prepended with zeros until it is 9 digits",
            "sample values": ["Customer#000000001", "Customer#000000090", "Customer#000000635", "Customer#000000892", "Customer#000000073"]
          },
          {
            "name": "address",
            "type": "table column",
            "column name": "c_address",
            "data type": "string",
            "description": "Address of the customer as a cryptographically encrypted string to anonymize the data.",
            "sample values": ["ZCWMiIFUwRZWX7Vr7BjZ,,BZbGeGOBe7n", "HcpYT5Ag 2I2QY,nSoP5F1LI"],
            "synonyms": ["location", "residence", "home address"]
          },
          {
            "name": "nation_key",
            "type": "table column",
            "column name": "c_nationkey",
            "data type": "numeric",
            "description": "Key from the nation that the customer belongs to",
            "sample values": [0, 1, 10, 13, 24],
            "synonyms": ["nation id"]
          },
          {
            "name": "phone",
            "type": "table column",
            "column name": "c_phone",
            "data type": "string",
            "description": "Phone number of the customer in the format 'CC-XXX-XXX-XXXX' where CC is the country code (each nation has a unique country code).",
            "sample values": ["19-962-391-7546", "24-413-105-9570", "31-703-857-4846", "34-591-761-1886"],
            "synonyms": ["contact number", "telephone number"]
          },
          {
            "name": "account_balance",
            "type": "table column",
            "column name": "c_acctbal",
            "data type": "numeric",
            "description": "The account balance of the customer in US dollars. The balance is rounded to the nearest cent and most of the values are between -$1,000 and +$10,000.",
            "sample values": [-998.22, 9999.72, 4510.35, 9125.21, -0.92, 58.93],
            "synonyms": ["balance", "credit", "wealth", "debt", "surplus", "cash on hand", "money in bank"]
          },
          {
            "name": "market_segment",
            "type": "table column",
            "column name": "c_mktsegment",
            "data type": "string",
            "description": "The market segment that the customer belongs to, which is always 'BUILDING', 'FURNITURE', 'AUTOMOBILE', 'MACHINERY' or 'HOUSEHOLD'",
            "sample values": ["BUILDING", "FURNITURE", "AUTOMOBILE", "MACHINERY", "HOUSEHOLD"],
            "synonyms": ["customer segment", "customer category", "market", "industry", "sector", "vertical"]
          },
          {
            "name": "comment",
            "type": "table column",
            "column name": "c_comment",
            "data type": "string",
            "description": "Commentary/remark on the customer"
          }
        ],
        "description": "The customers in the system within each nation. Each customer can have placed zero, one, or several orders.",
        "synonyms": ["citizens", "residents", "inhabitants", "consumers", "users", "buyers", "occupants"]
      }
    ],
    "relationships": [
      {
        "type": "simple join",
        "name": "nations",
        "parent collection": "regions",
        "child collection": "nations",
        "singular": false,
        "always matches": true,
        "keys": {"key": ["region_key"]},
        "description": "The nations contained within a region",
        "synonyms": ["countries"]
      },
      {
        "type": "reverse",
        "name": "region",
        "original parent": "regions",
        "original property": "nations",
        "singular": true,
        "always matches": true,
        "description": "The region that a nation is part of",
        "synonyms": ["continent", "segment of the world"]
      },
      {
        "type": "simple join",
        "name": "suppliers",
        "parent collection": "nations",
        "child collection": "suppliers",
        "singular": false,
        "always matches": true,
        "keys": {"key": ["nation_key"]},
        "description": "The suppliers belonging to a nation",
        "synonyms": ["companies", "producers", "businesses"]
      },
      {
        "type": "reverse",
        "name": "nation",
        "original parent": "nations",
        "original property": "suppliers",
        "singular": true,
        "always matches": true,
        "description": "The nation that a supplier belongs to",
        "synonyms": ["country", "state"]
      },
      {
        "type": "simple join",
        "name": "customers",
        "parent collection": "nations",
        "child collection": "customers",
        "singular": false,
        "always matches": true,
        "keys": {"key": ["nation_key"]},
        "description": "The customers belonging to a nation",
        "synonyms": ["citizens", "residents", "inhabitants", "consumers", "users", "buyers", "occupants"]
      },
      {
        "type": "reverse",
        "name": "nation",
        "original parent": "nations",
        "original property": "customers",
        "singular": true,
        "always matches": true,
        "description": "The nation that a customer belongs to",
        "synonyms": ["country", "state", "home"]
      },
      {
        "type": "simple join",
        "name": "supply_records",
        "parent collection": "parts",
        "child collection": "supply_records",
        "singular": false,
        "always matches": false,
        "keys": {"key": ["part_key"]},
        "description": "The records indicating which companies supply the part",
        "synonyms": ["producers", "vendors", "suppliers of part"]
      },
      {
        "type": "reverse",
        "name": "part",
        "original parent": "parts",
        "original property": "supply_records",
        "singular": true,
        "always matches": true,
        "description": "The part that a supply record belongs to",
        "synonyms": ["product", "item", "component"]
      },
      {
        "type": "simple join",
        "name": "lines",
        "parent collection": "parts",
        "child collection": "lines",
        "singular": false,
        "always matches": false,
        "keys": {"key": ["part_key"]},
        "description": "The line items for shipments of the part",
        "synonyms": ["shipments", "packages", "purchases", "deliveries", "sales"]
      },
      {
        "type": "reverse",
        "name": "part",
        "original parent": "parts",
        "original property": "lines",
        "singular": true,
        "always matches": true,
        "description": "The part that a line item contains, e.g. what part is being shipped as part of an order",
        "synonyms": ["product", "item", "component"]
      },
      {
        "type": "simple join",
        "name": "supply_records",
        "parent collection": "suppliers",
        "child collection": "supply_records",
        "singular": false,
        "always matches": false,
        "keys": {"key": ["supplier_key"]},
        "description": "The records indicating which parts the supplier supplies",
        "synonyms": ["product catalog", "inventory", "components supplied"]
      },
      {
        "type": "reverse",
        "name": "supplier",
        "original parent": "suppliers",
        "original property": "supply_records",
        "singular": true,
        "always matches": true,
        "description": "The supplier that a supply record belongs to",
        "synonyms": ["company", "producer", "business"]
      },
      {
        "type": "simple join",
        "name": "lines",
        "parent collection": "suppliers",
        "child collection": "lines",
        "singular": false,
        "always matches": false,
        "keys": {"key": ["supplier_key"]},
        "description": "The line items for shipments from the supplier, e.g. all purchases made from the supplier",
        "synonyms": ["shipments", "packages", "purchases", "deliveries", "sales"]
      },
      {
        "type": "reverse",
        "name": "supplier",
        "original parent": "suppliers",
        "original property": "lines",
        "singular": true,
        "always matches": true,
        "description": "The supplier that a line item contains, e.g. what supplier is the part being purchased from as part of an order",
        "synonyms": ["company", "producer", "business"]
      },
      {
        "type": "simple join",
        "name": "part_and_supplier",
        "parent collection": "lines",
        "child collection": "supply_records",
        "singular": true,
        "always matches": true,
        "keys": {"part_key": ["part_key"], "supplier_key": ["supplier_key"]},
        "description": "The corresponding entry in the supply records detailing more information about the supplier of the purchase and the part that was purchased",
        "synonyms": ["supply records", "supplier part information", "partsupp info"]
      },
      {
        "type": "reverse",
        "name": "lines",
        "original parent": "lines",
        "original property": "part_and_supplier",
        "singular": false,
        "always matches": false,
        "description": "The line item instances of a part/supplier combination being purchased by a customer",
        "synonym": ["shipments", "packages", "purchases", "deliveries", "line items"]
      },
      {
        "type": "simple join",
        "name": "order",
        "parent collection": "lines",
        "child collection": "orders",
        "singular": true,
        "always matches": true,
        "keys": {"order_key": ["key"]},
        "description": "The order that the line item belongs to"
      },
      {
        "type": "reverse",
        "name": "lines",
        "original parent": "lines",
        "original property": "order",
        "singular": false,
        "always matches": true,
        "description": "The line items that belong to an order, each representing the purchase of a specific part from a specific supplier",
        "synonyms": ["items", "order contents", "entries", "line items"]
      },
      {
        "type": "simple join",
        "name": "customer",
        "parent collection": "orders",
        "child collection": "customers",
        "singular": true,
        "always matches": true,
        "keys": {"customer_key": ["key"]},
        "description": "The customer that placed the order",
        "synonyms": ["buyer", "consumer", "user", "client"]
      },
      {
        "type": "reverse",
        "name": "orders",
        "original parent": "orders",
        "original property": "customer",
        "singular": false,
        "always matches": false,
        "description": "The orders that a customer has placed, each of which contains one or more line items",
        "synonyms": ["transactions", "purchases"]
      }
    ],
    "additional definitions": [],
    "verified pydough analysis": [],
    "extra semantic info": {}
  }
]
```
