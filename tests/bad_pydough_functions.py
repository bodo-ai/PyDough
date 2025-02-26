# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

import datetime
import math


def bad_bool_1():
    # Using `or`
    return Customer(
        is_eurasian=(nation.region.name == "EUROPE") or (nation.region.name == "ASIA")
    )


def bad_bool_2():
    # Using `and`
    return Parts.WHERE((size == 38) and CONTAINS(name, "green"))


def bad_bool_3():
    # Using `not`
    return Parts.WHERE(not STARTSWITH(size, "LG"))


def bad_window_1():
    # Missing `by`
    return Orders(RANKING())


def bad_window_2():
    # Empty `by`
    return Orders(PERCENTILE(by=()))


def bad_window_3():
    # Non-collations in `by`
    return Orders(RANKING(by=order_key))


def bad_window_4():
    # Non-positive levels
    return Orders(RANKING(by=order_key.ASC(), levels=0))


def bad_window_5():
    # Non-integer levels
    return Orders(RANKING(by=order_key.ASC(), levels="hello"))


def bad_window_6():
    # Non-positive n_buckets
    return Orders(PERCENTILE(by=order_key.ASC(), n_buckets=-3))


def bad_window_7():
    # Non-integer n_buckets
    return Orders(PERCENTILE(by=order_key.ASC(), n_buckets=[1, 2, 3]))


def bad_slice_1():
    # Unsupported slicing: negative stop
    return Customers(name[:-1])


def bad_slice_2():
    # Unsupported slicing: negative start
    return Customers(name[-5:])


def bad_slice_3():
    # Unsupported slicing: skipping
    return Customers(name[1:10:2])


def bad_slice_4():
    # Unsupported slicing: reversed
    return Customers(name[::-1])


def bad_lpad_1():
    # String length argument
    return Customers(padded_name=LPAD(name, "20", "*"))


def bad_lpad_2():
    # Empty padding string
    return Customers(padded_name=LPAD(name, 20, ""))


def bad_lpad_3():
    # Negative length
    return Customers(padded_name=LPAD(name, -5, "*"))


def bad_lpad_4():
    # Multi-character padding string
    return Customers(padded_name=LPAD(name, 20, "*#"))


def bad_lpad_5():
    # Non-integer length
    return Customers(padded_name=LPAD(name, 20.5, "*"))


def bad_lpad_6():
    # Non-integer length
    return Customers(padded_name=LPAD(name, datetime.datetime.now(), "*"))


def bad_lpad_7():
    # Non-literal length
    return Customers(padded_name=LPAD(name, LENGTH(phone), "*"))


def bad_lpad_8():
    # Non-literal padding string
    return Customers(padded_name=LPAD(name, 20, LENGTH(phone)))


def bad_rpad_1():
    # String length argument
    return Customers(padded_name=RPAD(name, "20", "*"))


def bad_rpad_2():
    # Empty padding string
    return Customers(padded_name=RPAD(name, 20, ""))


def bad_rpad_3():
    # Negative length
    return Customers(padded_name=RPAD(name, -5, "*"))


def bad_rpad_4():
    # Multi-character padding string
    return Customers(padded_name=RPAD(name, 20, "*#"))


def bad_rpad_5():
    # Non-integer length
    return Customers(padded_name=RPAD(name, 20.5, "*"))


def bad_rpad_6():
    # Non-integer length
    return Customers(padded_name=RPAD(name, datetime.datetime.now(), "*"))


def bad_rpad_7():
    # Non-literal length
    return Customers(padded_name=RPAD(name, LENGTH(phone), "*"))


def bad_rpad_8():
    # Non-literal padding string
    return Customers(padded_name=RPAD(name, 20, LENGTH(phone)))


def bad_floor():
    # Using `math.floor` (calls __floor__)
    return Customer(age=math.floor(order.total_price))


def bad_ceil():
    # Using `math.ceil` (calls __ceil__)
    return Customer(age=math.ceil(order.total_price))


def bad_trunc():
    # Using `math.trunc` (calls __trunc__)
    return Customer(age=math.trunc(order.total_price))


def bad_reversed():
    # Using `reversed` (calls __reversed__)
    return Regions(backwards_name=reversed(name))


def bad_int():
    # Casting to int (calls __int__)
    return Orders(limit=int(order.total_price))


def bad_float():
    # Casting to float (calls __float__)
    return Orders(limit=float(order.quantity))


def bad_complex():
    # Casting to complex (calls __complex__)
    return Orders(limit=complex(order.total_price))


def bad_index():
    # Using as an index (calls __index__)
    return Orders(s="ABCDE"[:order_priority])


def bad_nonzero():
    # Using in a boolean context (calls __nonzero__)
    return Lineitems(is_taxed=bool(tax))


def bad_len():
    # Using `len` (calls __len__)
    return Customers(len(customer.name))


def bad_contains():
    # Using `in` operator (calls __contains__)
    return Orders("discount" in order.details)


def bad_setitem():
    # Assigning to an index (calls __setitem__)
    order.details["discount"] = True
    return order


def bad_iter():
    # Iterating over an object (calls __iter__)
    for item in customer:
        print(item)
    return customer
