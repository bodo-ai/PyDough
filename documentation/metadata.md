# PyDough Metadata Spec

This page document the exact format that the JSON files containing PyDough metadata must ascribe to. The specification of this format is for the initial release of PyDough, but is intended to change drastically in the near future.

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [JSON File Structure](#json-file-structure)
- [Collections](#collections)
   * [Collection Type: Simple Table](#collection-type-simple-table)
- [Properties](#properties)
   * [Property Type: Table Column](#property-type-table-column)
   * [Property Type: Simple Join](#property-type-simple-join)
   * [Property Type: Cartesian Product](#property-type-cartesian-product)
   * [Property Type: Compound Relationship](#property-type-compound-relationship)
- [PyDough Type Strings](#pydough-type-strings)
- [Metadata Samples](#metadata-samples)
   * [Example: Education](#example-education)
   * [Example: Shipping](#example-shipping)

<!-- TOC end -->

<!-- TOC --><a name="json-file-structure"></a>
## JSON File Structure

The core components of the JSON file structure are as follows:

- Each JSON file for PyDough metadata is a JSON object where the keys are the names of the graphs in the file, and the values are JSON object representing the graphs themselves. The names of the graphs must be valid Python variable names. The graph names should not be duplicated within the file.
- Each graph JSON object has keys that are names of collections, and values that are JSON objects for the collections themselves. The collection names should not be duplicated within the file graph, should not be the same names as the graph itself, and must be valid Python variable names.
- Each collection JSON object can adhere to one of several different types of Collections, but all of these types have two required key-value pairs:
    * `type`: a string indicating what kind of collection this is. The value of this string must be one of the known names of types of collections, and informs what additional key-value pairs the JSON object must have.
    * `properties`: a JSON object of all properties of the collection, where the keys are the names of the properties and the values are JSON objects describing the properties.
- Each properties JSON object can adhere to one of several different types of Properties, but all of these types have one required key-value pair: 
    * `type`: a string indicating what kind of property this is. The value of this string must be one of the known names of types of collections, and informs what additional key-value pairs the JSON object must have.

<!-- TOC --><a name="collections"></a>
## Collections

<!-- TOC --><a name="collection-type-simple-table"></a>
### Collection Type: Simple Table

A collection with this type is essentially just a view of a table stored in whatever database is being used to execute the queries. Collections of this type have a type string of "simple_table" and the following additional key-value pairs in their metadata JSON object:

- `table_path`: a string indicating the fully-qualified path to reach the table based on where it is stored, e.g. `<db_name>.<schema_name>.<table_name>`. The table_name here does not need to be the same as the name of the collection.
- `unique_properties`: a list of JSON values indicating which properties are unique within the collection, meaning that no two rows of the table will have the same values. If a value in this list is a string, it means that property is unique within the collection. If a value in the list is a list of strings, it means that every combination of the properties in that list is unique within the collection.

The following types of properties are supported on this type of collection:

- `table_column`
- `simple_join`
- `cartesian_product`
- `compound`

<!-- TOC --><a name="properties"></a>
## Properties

<!-- TOC --><a name="property-type-table-column"></a>
### Property Type: Table Column

A property with this type is essentially just a view of a column in a table stored in whatever database is being used to execute the queries. Properties of this type are scalar expression attributes of the collection, as opposed to subcollections. Properties of this type have a type string of "table_column" and have the following additional key-value pairs in their metadata JSON object:

- `column_name`: a string indicating the name of the column within the table it comes from, which it can be different from the name of the property itself.
- `data_type`: a string indicating the PyDough type that this column corresponds to.

<!-- TOC --><a name="property-type-simple-join"></a>
### Property Type: Simple Join

A property with this type describes a subcollection of the current collection that is derived from performing an equi-join on two tables (e.g. SELECT ... FROM T1 JOIN T2 ON T1.a=T2.x AND T1.b = T2.y). Properties of this type are subcollections of the collection, as opposed to scalar attributes. If a collection has a property of this type, a corresponding property is added to the other collection to describe the reverse of the relationship. Properties of this type have a type string of "simple_join" and have the following additional key-value pairs in their metadata JSON object:

- `other_collection_name`: a string indicating the name of the other collection that this property connects the current collection to. This must be another collection in the same graph that supports simple_join properties.
- `singular`: a boolean that is true if each record in the current collection has at most 1 matching record of the subcollection, and false otherwise.
- `no_collisions`: a boolean that is true if multiple records from this collection can match onto the same record from the other collection, and false otherwise (true if-and-only-if the reverse relationship is singular).
- `keys`: a JSON object indicating the combinations of properties from this collection and the other collection that are compared for equality in order to determine join matches. The keys to this JSON object are the names of properties in the current collection, and the values are a list of 1+ strings that. are the names of properties in the other collection that they must be equal to in order to produce a match. All property names invoked in the keys object must correspond to scalar attributes of the collection, as opposed to being names of its subcollections. This object must be non-empty.
- `reverse_relationship_name`: the name of the property that is to be added to the other collection to describe the reverse version of this relationship. This string must be a valid property name but cannot be equal to another existing property name in the other collection.

<!-- TOC --><a name="property-type-cartesian-product"></a>
### Property Type: Cartesian Product

A property with this type describes a subcollection of the current collection that is derived from performing an cross-join on two collections (e.g. SELECT ... FROM T1, T2). Properties of this type are subcollections of the collection, as opposed to scalar attributes. If a collection has a property of this type, a corresponding property is added to the other collection to describe the reverse of the relationship. Properties of this type have a type string of "cartesian_product" and have the following additional key-value pairs in their metadata JSON object:

- `other_collection_name`: a string indicating the name of the other collection that this property connects the current collection to. This must be another collection in the same graph that supports cartesian_product properties.
- `reverse_relationship_name`: the name of the property that is to be added to the other collection to describe the reverse version of this relationship. This string must be a valid property name but cannot be equal to another existing property name in the other collection.

<!-- TOC --><a name="property-type-compound-relationship"></a>
### Property Type: Compound Relationship

A property with this type describes a subcollection of the current collection that is derived from combining a relationship from this collection to one of its subcollections with a relationship from that subcollection to one of its subcollections. Properties of this type are subcollections of the collection, as opposed to scalar attributes. If a collection has a property of this type, a corresponding property is added to the other collection to describe the reverse of the relationship, and the subcollection can inherit additional properties from the middle subcollection. Properties of this type have a type string of "compound" and have the following additional key-value pairs in their metadata JSON object:

- `primary_property`: a string indicating the name of a property that connects this collection to a subcollection.
- `secondary_property`: a string indicating the name of the property that connects the primary subcollection to one of its subcollections.
- `singular`: a boolean that is true if each record in the current collection has at most 1 matching record of the subcollection’s subcollection, and false otherwise.
- `no_collisions`: a boolean that is true if multiple records from this collection can match onto the same record from the subcollection’s subcollection, and false otherwise (true if-and-only-if the reverse relationship is singular).
- `reverse_relationship_name`: the name of the property that is to be added to the other collection to describe the reverse version of this relationship. This string must be a valid property name but cannot be equal to another existing property name in the other collection.
- `inherited_properties`: a JSON object indicating any properties of the primary subcollection that should be accessible from the secondary subcollection. The keys are the string names that the inherited properties are referred to by, which can be a new alias or the original name, and the values are names of the properties of the collectio accessed by the primary property. The names used for the inherited properties cannot overlap with any other names of properties of the secondary subcollection, including other inherited properties it could have from other compound relationships. This JSON object can be empty if there are no inherited properties.

<!-- TOC --><a name="pydough-type-strings"></a>
## PyDough Type Strings

The strings used in the type field for certain properties must be one of the following:

- `int8`: an 8-bit integer.
- `int16`: a 16-bit integer.
- `int32`: a 32-bit integer.
- `int64`: a 64-bit integer.
- `float32`: a 32-bit float.
- `float64`: a 64-bit float.
- `decimal[p,s]`: a fixed-point number with precision p (integer between 1 and 38) indicating the number of digits and scale s (integer between 0 and p, exclusive of p) indicating the number of digits to the right hand side of the decimal point.
- `bool`: a boolean.
- `string`: a string.
- `binary`: a bytes format.
- `date`: a date without any time component.
- `time[p]`: a time of day with a precision p (integer between 0 and 9) indicating the number of sub-second decimal places it can have (e.g. 9 = nanosecond precision).
- `timestamp[p]`: a date with a time of day of a precision p (same rules as time).
- `timestamp[p,tz]`: a date with a time of day of a precision p (same rules as time) with the timezone described by tz (e.g. `timestamp[9,America/Los_Angeles]`).
- `array[t]`: an array of values of type t (where t is another PyDough type). For example: `array[int32]` or `array[array[string]]`.
- `map[t1,t2]`: a map of values with keys of type type t1 and values of type t2 (where t1 and t2 are also PyDough types). For example: `map[string,int64]` or `map[string,array[date]]`.
- `struct[field1:t1,field2:t2,...]`: an struct of values with fields named field1, field2, etc. with types t1, t2, etc. (which are also PyDough types). For example: `struct[x:int32,y:int32]` or `struct[name:string,birthday:date,car_accidents:array[struct[ts:timestamp[9],report:string]]`. Each field name must be a valid Python identifier.
- `unkown`: an unknown type.

<!-- TOC --><a name="metadata-samples"></a>
## Metadata Samples


<!-- TOC --><a name="example-education"></a>
### Example: Education

The knowledge graph is for the following information about tables in a schema called `education`:
- `PEOPLE` is a table referring to each known person. Each row in this table has the following information:
    - `p_ssn`: the social security number used to uniquely identify the person.
    - `p_name`: the name of the person.
- `SCHOOLS` is a table referring to each known school. Each row in this table has the following information:
    - `s_id`: the identifying number used to uniquely identify the school.
    - `s_name`: the name of the school.
- `ATTENDANCE` is a table referring to every known instance of a person attending a school (assuming a person could have attended multiple schools, but attended each school at most once). Each row in this table has the following information:
    - `a_ssn`: the social security number of the person attending the school.
    - `a_id`: the id of the school being attended.
    - `a_gpa`: the GPA that the person had while attending the school.
    - `a_graddate`: the date that the person graduated from the school.
    - `a_degree`: the degree the person earned from teh school.
- Records in `PEOPLE` and `ATTENDANCE` can be joined on the social security number.
- Records in `SCHOOLS` and `ATTENDANCE` can be joined on the school id.

```json
{
  "Education": {
    "People": {
      "type": "simple_table",
      "table_path": "education.PEOPLE",
      "unique_properties": ["p_ssn"],
      "properties": {
        "ssn": {
          "type": "table_column",
          "column_name": "p_ssn",
          "data_type": "string"
        },
        "name": {
          "type": "table_column",
          "column_name": "p_name",
          "data_type": "string"
        },
        "attendances": {
          "type": "simple_join",
          "other_collection_name": "Attendance",
          "singular": false,
          "no_collisions": true,
          "keys": {
            "p_ssn": ["a_ssn"]
          },
          "reverse_relationship_name": "person"
        },
        "schools_attended": {
          "type": "compound",
          "primary_property": "attendances",
          "secondary_property": "school",
          "singular": false,
          "no_collisions": false,
          "inherited_properties": {
            "gpa": "gpa",
            "graduation_date": "graduation_date",
            "degree": "degree"
          },
          "reverse_relationship_name": "students"
        }
      }
    },
    "Schools": {
      "type": "simple_table",
      "table_path": "education.SCHOOLS",
      "unique_properties": ["school_id"],
      "properties": {
        "school_id": {
          "type": "table_column",
          "column_name": "s_id",
          "data_type": "string"
        },
        "name": {
          "type": "table_column",
          "column_name": "s_name",
          "data_type": "string"
        },
        "attendances": {
          "type": "simple_join",
          "other_collection_name": "Attendance",
          "singular": false,
          "no_collisions": true,
          "keys": {
            "school_id": ["school_id"]
          },
          "reverse_relationship_name": "school"
        }
      }
    },
    "Attendance": {
      "type": "simple_table",
      "table_path": "education.ATTENDANCE",
      "unique_properties": ["person_ssn", "school_id"],
      "properties": {
        "person_ssn": {
          "type": "table_column",
          "column_name": "a_ssn",
          "data_type": "string"
        },
        "school_id": {
          "type": "table_column",
          "column_name": "a_id",
          "data_type": "string"
        },
        "gpa": {
          "type": "table_column",
          "column_name": "a_gpa",
          "data_type": "float64"
        },
        "graduation_date": {
          "type": "table_column",
          "column_name": "a_graddate",
          "data_type": "date"
        },
        "degree": {
          "type": "table_column",
          "column_name": "a_degree",
          "data_type": "string"
        }
      }
    }
  }
}
```

The PyDough metadata representation shown in this JSON file corresponds to the following set of collections:
- `People` (corresponds to `education.PEOPLE`; records are unique per unique value of `ssn`). Has the following properties:
    - `ssn` (scalar property)
    - `name` (scalar property)
    - `attendances` (sub-collection connecting to `Attendances`)
    - `schools` (sub-collection connecting to `Schools` by going through `Attendances`)
- `Schools` (corresponds to `education.SCHOOLS`; records are unique per unique value of `school_id`). Has the following properties:
    - `school_id` (scalar property)
    - `school_name` (scalar property)
    - `attendances` (sub-collection connecting to `Attendances`)
    - `students` (sub-collection connecting to `People` by going through `Attendances`, reverse of `People.schools`)
- `Attendances` (corresponds to `education.ATTENDANCES`; records are unique per unique combination of `person_ssn` & `school_id`). Has the following properties:
    - `person_ssn` (scalar property)
    - `school_id` (scalar property)
    - `gpa` (scalar property)
    - `graduation_date` (scalar property)
    - `degree` (scalar property)
    - `person` (sub-collection connecting to `People`, reverse of `People.attendances`)
    - `school` (sub-collection connecting to `School`, reverse of `Schools.attendances`)

<!-- TOC --><a name="example-shipping"></a>
### Example: Shipping

The knowledge graph is for the following information about tables in a schema called `shipping`:
- `PEOPLE` is a table referring to each known person. Each row in this table has the following information:
    - `ssn`: the social security number used to uniquely identify the person.
    - `first`: the first name of the person.
    - `middle`: the middle name of the person, if they have one.
    - `last`: the last name of the person.
    - `bdate`: the date that the person was born.
    - `email`: the email of the person.
    - `ca_id`: the id of the person's current address, if they have one.
- `PACKAGES` is a table referring to each known package. Each row in this table has the following information:
    - `pid`: the identifying number used to uniquely identify the package.
    - `cust_ssn`: the social security number of the person who ordered the package.
    - `ship_id`: the address id of the address the package was shipped to.
    - `bill_id`: the address id of the address the package was billed to.
    - `order_date`: the date that the package was ordered.
    - `arrival_date`: the date that the package arrived, if it has.
    - `cost`: the total cost of the package.
- `ADDRESSES` is a table referring to every known address. Each row in this table has the following information:
    - `aid`: the identifying number used to uniquely identify the address.
    - `number`: the street number of the address.
    - `street`: the name of the street of the the address.
    - `apartment`: the name of the apartment, if the address has one.
    - `zip`: the zip code of the address.
    - `city`: the city containing the address.
    - `state`: the state containing the address.
- Records in `PEOPLE` and `PACKAGES` can be joined on the social security number.
- Records in `PEOPLE` and `ADDRESSES` can be joined on the address id.
- Records in `PACKAGES` and `ADDRESSES` can be joined on either the shipping address id or the billing address id.

```json
{
  "Shipping": {
    "People": {
      "type": "simple_table",
      "table_path": "shipping.PEOPLE",
      "unique_properties": ["ssn"],
      "properties": {
        "ssn": {
          "type": "table_column",
          "column_name": "ssn",
          "data_type": "string"
        },
        "first_name": {
          "type": "table_column",
          "column_name": "first",
          "data_type": "string"
        },
        "middle_name": {
          "type": "table_column",
          "column_name": "middle",
          "data_type": "string"
        },
        "last_name": {
          "type": "table_column",
          "column_name": "last",
          "data_type": "string"
        },
        "birth_date": {
          "type": "table_column",
          "column_name": "bdate",
          "data_type": "date"
        },
        "email": {
          "type": "table_column",
          "column_name": "email",
          "data_type": "string"
        },
        "ca_id": {
          "type": "table_column",
          "column_name": "email",
          "data_type": "int64"
        },
        "current_address": {
          "type": "simple_join",
          "other_collection_name": "Addresses",
          "singular": true,
          "no_collisions": false,
          "keys": {
            "ca_id": ["aid"]
          },
          "reverse_relationship_name": "current_occupants"
        },
        "packages_ordered": {
          "type": "simple_join",
          "other_collection_name": "Packages",
          "singular": false,
          "no_collisions": true,
          "keys": {
            "ssn": ["cust_ssn"]
          },
          "reverse_relationship_name": "customer"
        }
      }
    },
    "Packages": {
      "type": "simple_table",
      "table_path": "shipping.PACKAGES",
      "unique_properties": ["pid"],
      "properties": {
        "package_id": {
          "type": "table_column",
          "column_name": "pid",
          "data_type": "int64"
        },
        "customer_ssn": {
          "type": "table_column",
          "column_name": "cust_ssn",
          "data_type": "string"
        },
        "shipping_address_id": {
          "type": "table_column",
          "column_name": "ship_id",
          "data_type": "int64"
        },
        "billing_address_id": {
          "type": "table_column",
          "column_name": "ship_id",
          "data_type": "int64"
        },
        "order_date": {
          "type": "table_column",
          "column_name": "order_date",
          "data_type": "date"
        },
        "arrival_date": {
          "type": "table_column",
          "column_name": "arrival_date",
          "data_type": "date"
        },
        "cost": {
          "type": "table_column",
          "column_name": "cost",
          "data_type": "decimal[10,2]"
        },
        "shipping_address": {
          "type": "simple_join",
          "other_collection_name": "Addresses",
          "singular": true,
          "no_collisions": false,
          "keys": {
            "shipping_address_id": ["aid"]
          },
          "reverse_relationship_name": "shipped_packages"
        },
        "billing_address": {
          "type": "simple_join",
          "other_collection_name": "Addresses",
          "singular": true,
          "no_collisions": false,
          "keys": {
            "billing_address_id": ["aid"]
          },
          "reverse_relationship_name": "billed_packages"
        }
      }
    },
    "Addresses": {
      "type": "simple_table",
      "table_path": "shipping.ADDRESSES",
      "unique_properties": ["aid"],
      "properties": {
        "address_id": {
          "type": "table_column",
          "column_name": "aid",
          "data_type": "string"
        },
        "street_number": {
          "type": "table_column",
          "column_name": "street_number",
          "data_type": "string"
        },
        "street_name": {
          "type": "table_column",
          "column_name": "street_name",
          "data_type": "string"
        },
        "apartment": {
          "type": "table_column",
          "column_name": "apartment",
          "data_type": "string"
        },
        "zip_code": {
          "type": "table_column",
          "column_name": "zip",
          "data_type": "string"
        },
        "city": {
          "type": "table_column",
          "column_name": "city",
          "data_type": "string"
        },
        "state": {
          "type": "table_column",
          "column_name": "state",
          "data_type": "string"
        }
      }
    }
  }
}
```

The PyDough metadata representation shown in this JSON file corresponds to the following set of collections:
- `People` (corresponds to `shipping.PEOPLE`; records are unique per unique value of `ssn`). Has the following properties:
    - `ssn` (scalar property)
    - `first_name` (scalar property)
    - `middle_name` (scalar property)
    - `last_name` (scalar property)
    - `birth_date` (scalar property)
    - `email` (scalar property)
    - `current_address_id` (scalar property)
    - `current_address` (sub-collection connecting to `Addresses`)
    - `packages_ordered` (sub-collection connecting to `Packages`)
- `Packages` (corresponds to `shipping.PACKAGES`; records are unique per unique value of `pid`). Has the following properties:
    - `package_id` (scalar property)
    - `customer_ssn` (scalar property)
    - `shipping_address_id` (scalar property)
    - `billing_address_id` (scalar property)
    - `order_date` (scalar property)
    - `arrival_date` (scalar property)
    - `cost` (scalar property)
    - `shipping_address` (sub-collection connecting to `Addresses`)
    - `billing_address` (sub-collection connecting to `Addresses`)
- `Addresses` (corresponds to `shipping.ADDRESSES`; records are unique per unique value of `aid`). Has the following properties:
    - `address_id` (scalar property)
    - `street_number` (scalar property)
    - `street_name` (scalar property)
    - `apartment` (scalar property)
    - `zip_code` (scalar property)
    - `city` (scalar property)
    - `state` (scalar property)
    - `current_occupants` (sub-collection connecting to `People`)
    - `shipped_packages` (sub-collection connecting to `Packages`)
    - `billed_packages` (sub-collection connecting to `Packages`)
