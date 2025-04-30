# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

import datetime
import math
import datetime


def bad_bool_1():
    # Using `or`
    return Customer(
        is_eurasian=(nation.region.name == "EUROPE") or (nation.region.name == "ASIA")
    )


def bad_bool_2():
    # Using `and`
    return parts.WHERE((size == 38) and CONTAINS(name, "green"))


def bad_bool_3():
    # Using `not`
    return parts.WHERE(not STARTSWITH(size, "LG"))


def bad_window_1():
    # Missing `by`
    return orders.CALCULATE(RANKING())


def bad_window_2():
    # Empty `by`
    return orders.CALCULATE(PERCENTILE(by=()))


def bad_window_3():
    # Non-positive n_buckets
    return orders.CALCULATE(PERCENTILE(by=order_key.ASC(), n_buckets=-3))


def bad_window_4():
    # Non-integer n_buckets
    return orders.CALCULATE(PERCENTILE(by=order_key.ASC(), n_buckets=[1, 2, 3]))


def bad_lpad_1():
    # String length argument
    return customers.CALCULATE(padded_name=LPAD(name, "20", "*"))


def bad_lpad_2():
    # Empty padding string
    return customers.CALCULATE(padded_name=LPAD(name, 20, ""))


def bad_lpad_3():
    # Negative length
    return customers.CALCULATE(padded_name=LPAD(name, -5, "*"))


def bad_lpad_4():
    # Multi-character padding string
    return customers.CALCULATE(padded_name=LPAD(name, 20, "*#"))


def bad_lpad_5():
    # Non-integer length
    return customers.CALCULATE(padded_name=LPAD(name, 20.5, "*"))


def bad_lpad_6():
    # Non-integer length
    return customers.CALCULATE(padded_name=LPAD(name, datetime.datetime.now(), "*"))


def bad_lpad_7():
    # Non-literal length
    return customers.CALCULATE(padded_name=LPAD(name, LENGTH(phone), "*"))


def bad_lpad_8():
    # Non-literal padding string
    return customers.CALCULATE(padded_name=LPAD(name, 20, LENGTH(phone)))


def bad_rpad_1():
    # String length argument
    return customers.CALCULATE(padded_name=RPAD(name, "20", "*"))


def bad_rpad_2():
    # Empty padding string
    return customers.CALCULATE(padded_name=RPAD(name, 20, ""))


def bad_rpad_3():
    # Negative length
    return customers.CALCULATE(padded_name=RPAD(name, -5, "*"))


def bad_rpad_4():
    # Multi-character padding string
    return customers.CALCULATE(padded_name=RPAD(name, 20, "*#"))


def bad_rpad_5():
    # Non-integer length
    return customers.CALCULATE(padded_name=RPAD(name, 20.5, "*"))


def bad_rpad_6():
    # Non-integer length
    return customers.CALCULATE(padded_name=RPAD(name, datetime.datetime.now(), "*"))


def bad_rpad_7():
    # Non-literal length
    return customers.CALCULATE(padded_name=RPAD(name, LENGTH(phone), "*"))


def bad_rpad_8():
    # Non-literal padding string
    return customers.CALCULATE(padded_name=RPAD(name, 20, LENGTH(phone)))


def bad_slice_1():
    # Unsupported slicing: skipping
    return customers.CALCULATE(name[1:10:2])


def bad_slice_2():
    # Unsupported slicing: reversed
    return customers.CALCULATE(name[::-1])


def bad_slice_3():
    # Unsupported slicing: non-integer start
    return customers.CALCULATE(name["invalid":-1:])


def bad_slice_4():
    # Unsupported slicing: non-integer start
    return customers.CALCULATE(name[datetime.datetime.now() : -1 :])


def bad_slice_5():
    # Unsupported slicing: non-integer start
    return customers.CALCULATE(name[42.4:10:])


def bad_slice_6():
    # Unsupported slicing: non-integer stop
    return customers.CALCULATE(name[1:"invalid":])


def bad_slice_7():
    # Unsupported slicing: non-integer stop
    return customers.CALCULATE(name[1 : datetime.datetime.now() :])


def bad_slice_8():
    # Unsupported slicing: non-integer stop
    return customers.CALCULATE(name[1:42.4:])


def bad_slice_9():
    # Unsupported slicing: non-integer step
    return customers.CALCULATE(name[1:10:"invalid"])


def bad_slice_10():
    # Unsupported slicing: non-integer step
    return customers.CALCULATE(name[1 : 10 : datetime.datetime.now()])


def bad_slice_11():
    # Unsupported slicing: non-integer step
    return customers.CALCULATE(name[1:10:42.4])


def bad_slice_12():
    # Unsupported slicing: non-integer start
    return customers.CALCULATE(name[LENGTH(name) : 10 :])


def bad_slice_13():
    # Unsupported slicing: non-integer step
    return customers.CALCULATE(name[1 : LENGTH(name) :])


def bad_slice_14():
    # Unsupported slicing: non-integer step
    return customers.CALCULATE(name[1 : 10 : LENGTH(name)])


def bad_floor():
    # Using `math.floor` (calls __floor__)
    return customers.CALCULATE(age=math.floor(order.total_price))


def bad_ceil():
    # Using `math.ceil` (calls __ceil__)
    return customers.CALCULATE(age=math.ceil(order.total_price))


def bad_trunc():
    # Using `math.trunc` (calls __trunc__)
    return customers.CALCULATE(age=math.trunc(order.total_price))


def bad_reversed():
    # Using `reversed` (calls __reversed__)
    return regions.CALCULATE(backwards_name=reversed(name))


def bad_int():
    # Casting to int (calls __int__)
    return orders.CALCULATE(limit=int(order.total_price))


def bad_float():
    # Casting to float (calls __float__)
    return orders.CALCULATE(limit=float(order.quantity))


def bad_complex():
    # Casting to complex (calls __complex__)
    return orders.CALCULATE(limit=complex(order.total_price))


def bad_index():
    # Using as an index (calls __index__)
    return orders.CALCULATE(s="ABCDE"[:order_priority])


def bad_nonzero():
    # Using in a boolean context (calls __nonzero__)
    return lines.CALCULATE(is_taxed=bool(tax))


def bad_len():
    # Using `len` (calls __len__)
    return customers.CALCULATE(len(customer.name))


def bad_contains():
    # Using `in` operator (calls __contains__)
    return orders.CALCULATE("discount" in comment)


def bad_setitem():
    # Assigning to an index (calls __setitem__)
    orders["discount"] = True
    return orders


def bad_iter():
    # Iterating over an object (calls __iter__)
    for item in customers:
        print(item)
    return customers


def bad_round1():
    return DailyPrices.CALCULATE(rounded_high=ROUND(high, 0.5))


def bad_round2():
    return DailyPrices.CALCULATE(rounded_high=ROUND(high, -0.5, 2))
