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
    return Parts.WHERE((size == 38) and CONTAINS(name, "green"))


def bad_bool_3():
    # Using `not`
    return Parts.WHERE(not STARTSWITH(size, "LG"))


def bad_window_1():
    # Missing `by`
    return Orders.CALCULATE(RANKING())


def bad_window_2():
    # Empty `by`
    return Orders.CALCULATE(PERCENTILE(by=()))


def bad_window_3():
    # Non-positive n_buckets
    return Orders.CALCULATE(PERCENTILE(by=order_key.ASC(), n_buckets=-3))


def bad_window_4():
    # Non-integer n_buckets
    return Orders.CALCULATE(PERCENTILE(by=order_key.ASC(), n_buckets=[1, 2, 3]))


def bad_window_5():
    # Relsum with by but without cumulative/frame
    return Customers.CALCULATE(RELSUM(acctbal, by=acctbal.ASC()))


def bad_window_6():
    # Relavg with cumulative but without by
    return Customers.CALCULATE(RELAVG(acctbal, cumulative=True))


def bad_window_7():
    # Relcount with frame but without by
    return Customers.CALCULATE(RELCOUNT(acctbal, frame=(-10, 0)))


def bad_window_8():
    # Relsize with frame AND cumulative
    return Customers.CALCULATE(
        RELSIZE(by=acctbal.ASC(), cumulative=True, frame=(-10, 0))
    )


def bad_window_9():
    # Relsum with malformed frame: not an integer/null
    return Customers.CALCULATE(RELSUM(by=acctbal.ASC(), frame=(-10.5, 0)))


def bad_window_10():
    # Relsum with malformed frame: lower > upper
    return Customers.CALCULATE(RELSUM(by=acctbal.ASC(), frame=(0, -1)))


def bad_lpad_1():
    # String length argument
    return Customers.CALCULATE(padded_name=LPAD(name, "20", "*"))


def bad_lpad_2():
    # Empty padding string
    return Customers.CALCULATE(padded_name=LPAD(name, 20, ""))


def bad_lpad_3():
    # Negative length
    return Customers.CALCULATE(padded_name=LPAD(name, -5, "*"))


def bad_lpad_4():
    # Multi-character padding string
    return Customers.CALCULATE(padded_name=LPAD(name, 20, "*#"))


def bad_lpad_5():
    # Non-integer length
    return Customers.CALCULATE(padded_name=LPAD(name, 20.5, "*"))


def bad_lpad_6():
    # Non-integer length
    return Customers.CALCULATE(padded_name=LPAD(name, datetime.datetime.now(), "*"))


def bad_lpad_7():
    # Non-literal length
    return Customers.CALCULATE(padded_name=LPAD(name, LENGTH(phone), "*"))


def bad_lpad_8():
    # Non-literal padding string
    return Customers.CALCULATE(padded_name=LPAD(name, 20, LENGTH(phone)))


def bad_rpad_1():
    # String length argument
    return Customers.CALCULATE(padded_name=RPAD(name, "20", "*"))


def bad_rpad_2():
    # Empty padding string
    return Customers.CALCULATE(padded_name=RPAD(name, 20, ""))


def bad_rpad_3():
    # Negative length
    return Customers.CALCULATE(padded_name=RPAD(name, -5, "*"))


def bad_rpad_4():
    # Multi-character padding string
    return Customers.CALCULATE(padded_name=RPAD(name, 20, "*#"))


def bad_rpad_5():
    # Non-integer length
    return Customers.CALCULATE(padded_name=RPAD(name, 20.5, "*"))


def bad_rpad_6():
    # Non-integer length
    return Customers.CALCULATE(padded_name=RPAD(name, datetime.datetime.now(), "*"))


def bad_rpad_7():
    # Non-literal length
    return Customers.CALCULATE(padded_name=RPAD(name, LENGTH(phone), "*"))


def bad_rpad_8():
    # Non-literal padding string
    return Customers.CALCULATE(padded_name=RPAD(name, 20, LENGTH(phone)))


def bad_slice_1():
    # Unsupported slicing: skipping
    return Customers.CALCULATE(name[1:10:2])


def bad_slice_2():
    # Unsupported slicing: reversed
    return Customers.CALCULATE(name[::-1])


def bad_slice_3():
    # Unsupported slicing: non-integer start
    return Customers.CALCULATE(name["invalid":-1:])


def bad_slice_4():
    # Unsupported slicing: non-integer start
    return Customers.CALCULATE(name[datetime.datetime.now() : -1 :])


def bad_slice_5():
    # Unsupported slicing: non-integer start
    return Customers.CALCULATE(name[42.4:10:])


def bad_slice_6():
    # Unsupported slicing: non-integer stop
    return Customers.CALCULATE(name[1:"invalid":])


def bad_slice_7():
    # Unsupported slicing: non-integer stop
    return Customers.CALCULATE(name[1 : datetime.datetime.now() :])


def bad_slice_8():
    # Unsupported slicing: non-integer stop
    return Customers.CALCULATE(name[1:42.4:])


def bad_slice_9():
    # Unsupported slicing: non-integer step
    return Customers.CALCULATE(name[1:10:"invalid"])


def bad_slice_10():
    # Unsupported slicing: non-integer step
    return Customers.CALCULATE(name[1 : 10 : datetime.datetime.now()])


def bad_slice_11():
    # Unsupported slicing: non-integer step
    return Customers.CALCULATE(name[1:10:42.4])


def bad_slice_12():
    # Unsupported slicing: non-integer start
    return Customers.CALCULATE(name[LENGTH(name) : 10 :])


def bad_slice_13():
    # Unsupported slicing: non-integer step
    return Customers.CALCULATE(name[1 : LENGTH(name) :])


def bad_slice_14():
    # Unsupported slicing: non-integer step
    return Customers.CALCULATE(name[1 : 10 : LENGTH(name)])


def bad_floor():
    # Using `math.floor` (calls __floor__)
    return Customers.CALCULATE(age=math.floor(order.total_price))


def bad_ceil():
    # Using `math.ceil` (calls __ceil__)
    return Customers.CALCULATE(age=math.ceil(order.total_price))


def bad_trunc():
    # Using `math.trunc` (calls __trunc__)
    return Customers.CALCULATE(age=math.trunc(order.total_price))


def bad_reversed():
    # Using `reversed` (calls __reversed__)
    return Regions.CALCULATE(backwards_name=reversed(name))


def bad_int():
    # Casting to int (calls __int__)
    return Orders.CALCULATE(limit=int(order.total_price))


def bad_float():
    # Casting to float (calls __float__)
    return Orders.CALCULATE(limit=float(order.quantity))


def bad_complex():
    # Casting to complex (calls __complex__)
    return Orders.CALCULATE(limit=complex(order.total_price))


def bad_index():
    # Using as an index (calls __index__)
    return Orders.CALCULATE(s="ABCDE"[:order_priority])


def bad_nonzero():
    # Using in a boolean context (calls __nonzero__)
    return Lineitems.CALCULATE(is_taxed=bool(tax))


def bad_len():
    # Using `len` (calls __len__)
    return Customers.CALCULATE(len(customer.name))


def bad_contains():
    # Using `in` operator (calls __contains__)
    return Orders.CALCULATE("discount" in comment)


def bad_setitem():
    # Assigning to an index (calls __setitem__)
    Orders["discount"] = True
    return Orders


def bad_iter():
    # Iterating over an object (calls __iter__)
    for item in Customers:
        print(item)
    return Customers


def bad_round1():
    return DailyPrices.CALCULATE(rounded_high=ROUND(high, 0.5))


def bad_round2():
    return DailyPrices.CALCULATE(rounded_high=ROUND(high, -0.5, 2))


def bad_unsupported_kwarg1():
    # Unsupported value of supported keyword argument
    return Nations.CALCULATE(var1=VAR(suppliers.account_balance, type="wrong_type"))


def bad_unsupported_kwarg2():
    # Unsupported keyword argument
    return Nations.CALCULATE(var1=VAR(suppliers.account_balance, wrong_type="sample"))


def bad_unsupported_kwarg3():
    # Unsupported keyword argument for non keyword operator
    return Nations.CALCULATE(sum=SUM(suppliers.account_balance, wrong_kwarg="sample"))
