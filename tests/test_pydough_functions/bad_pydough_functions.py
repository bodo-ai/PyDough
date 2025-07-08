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
    return daily_prices.CALCULATE(rounded_high=ROUND(high, 0.5))


def bad_round2():
    return daily_prices.CALCULATE(rounded_high=ROUND(high, -0.5, 2))


def bad_unsupported_kwarg1():
    # Unsupported value of supported keyword argument
    return nations.CALCULATE(var1=VAR(suppliers.account_balance, type="wrong_type"))


def bad_unsupported_kwarg2():
    # Unsupported keyword argument
    return nations.CALCULATE(var1=VAR(suppliers.account_balance, wrong_type="sample"))


def bad_unsupported_kwarg3():
    # Unsupported keyword argument for non keyword operator
    return nations.CALCULATE(sum=SUM(suppliers.account_balance, wrong_kwarg="sample"))


# Tets for the error messages when accessing a term that does not exist
def bad_name_1():
    return customers.CALCULATE(c_name)


def bad_name_2():
    return TPCH.CUSTS


def bad_name_3():
    return TPCH.CALCULATE(foo=1, bar=2, fizz=3, BUZZ=4).CALCULATE(fizzbuzz)


# collection.collection.property_last_collection
def bad_name_4():
    return customers.orders.CALCULATE(totalPrice)


# collection.collection.property_of_first_collection
def bad_name_5():
    return customers.orders.CALCULATE(c_name)


# collection suppliers doesn't exist
def bad_name_6():
    return customers.suppliers


# test same with all uppercase
def bad_name_7():
    return customers.CALCULATE(NAME)


# test same with numbers
def bad_name_8():
    return customers.CALCULATE(n123ame)


# test with underscores
def bad_name_9():
    return customers.CALCULATE(with_underscores=__phone__)


# test without just one chatacter
def bad_name_10():
    return customers.CALCULATE(nam)


# test with an extra character
def bad_name_11():
    return customers.CALCULATE(namex)


# test with all underscores
def bad_name_12():
    return customers.CALCULATE(c_name=___)


# test with a really large name
def bad_name_13():
    return customers.CALCULATE(
        really_long_name=thisisareallylargename_that_exceeds_the_system_limit
    )


# test with the combination of to names declared (key, name). keyname
def bad_name_14():
    return customers.CALCULATE(keyname)


# test with the combination of to names declared (key, name). same as above
# but the other way around
def bad_name_15():
    return customers.CALCULATE(namekey)


# More than one bad name CALCULATE(customer.c_name, customes.bad_key).not_collection
def bad_name_16():
    return customers.CALCULATE(name, no_exist).orrders


# TEST for PARTITION
def bad_name_17():
    return orders.CALCULATE(year=YEAR(order_date)).PARTITION(name="years", by=year).yrs


def bad_name_18():
    return (
        orders.CALCULATE(year=YEAR(order_date))
        .PARTITION(name="years", by=year)
        .CALCULATE(n_orders=COUNT(orders))
        .orders.nords
    )


def bad_name_19():
    return (
        orders.CALCULATE(year=YEAR(order_date))
        .PARTITION(name="years", by=year)
        .order_date
    )


def bad_name_20():
    return (
        orders.CALCULATE(year=YEAR(order_date))
        .PARTITION(name="years", by=year)
        .CALCULATE(n_orders=COUNT(orders))
        .orders.orders
    )


def bad_name_21():
    return regions.CALCULATE(rname=name, rkey=key, rcomment=comment).nations.RNAME


def bad_name_22():
    return TPCH.CALCULATE(
        anthro_pomorph_IZATION=1,
        counte_rintelligence=2,
        OVERIN_tellectualizers=3,
        ultra_revolution_aries=4,
        PROFESSION_alization=5,
        De_Institutionalizations=6,
        over_intellect_ualiz_ation=7,
    ).CALCULATE(Over_Intellectual_Ization)


def bad_name_23():
    return TPCH.CALCULATE(
        anthro_pomorph_IZATION=1,
        counte_rintelligence=2,
        OVERIN_tellectualizers=3,
        ultra_revolution_aries=4,
        PROFESSION_alization=5,
        De_Institutionalizations=6,
        over_intellect_ualiz_ation=7,
    ).CALCULATE(paio_eo_aliz_ation)


def bad_name_24():
    return TPCH.CALCULATE(
        anthro_pomorph_IZATION=1,
        counte_rintelligence=2,
        OVERIN_tellectualizers=3,
        ultra_revolution_aries=4,
        PROFESSION_alization=5,
        De_Institutionalizations=6,
        over_intellect_ualiz_ation=7,
    ).CALCULATE(_a_r_h_x_n_t_p_o_q__z_m_o_p_i__a_o_n_z_)


def bad_name_25():
    return TPCH.CALCULATE(
        anthro_pomorph_IZATION=1,
        counte_rintelligence=2,
        OVERIN_tellectualizers=3,
        ultra_revolution_aries=4,
        PROFESSION_alization=5,
        De_Institutionalizations=6,
        over_intellect_ualiz_ation=7,
    ).CALCULATE(
        anthropomorphization_and_overintellectualization_and_ultrarevolutionaries
    )


def bad_sqlite_udf_1():
    # Calling a UDF that requires 2 arguments with only 1 argument
    return orders.CALCULATE(x=FORMAT_DATETIME("%Y"))


def bad_sqlite_udf_2():
    # Calling a UDF that requires 2 arguments with 3 arguments
    return orders.CALCULATE(x=FORMAT_DATETIME("%Y", order_date, "foo"))


def bad_sqlite_udf_3():
    # Calling a UDF that requires 1-2 arguments with 0 arguments
    return nations.CALCULATE(x=GCAT(by=name.ASC()))


def bad_sqlite_udf_4():
    # Calling a UDF that requires 1-2 arguments with 3 arguments
    return nations.CALCULATE(x=GCAT(name, ";", "bar", by=name.ASC()))


# TEST for CROSS
def bad_cross_1():
    # Reason it is bad: Using `CROSS` with a not a collection
    return customers.CROSS(42)


def bad_cross_2():
    # Reason it is bad: not a collection
    return COUNT(customers).CROSS(regions)


def bad_cross_3():
    # Reason it is bad: the RHS isn't valid PyDough code by itself
    return customers.CROSS(foo)


def bad_cross_4():
    # Reason it is bad: name collision
    return regions.CALCULATE(customers=COUNT(nations.customers)).CROSS(customers)


def bad_cross_5():
    # Reason it is bad: name collision
    r = regions.CALCULATE(name)
    return r.CROSS(r).CALCULATE(name2=r.name)


def bad_cross_6():
    # Reason it is bad: CROSS unrelated collections
    return suppliers.CROSS(parts).CALCULATE(
        sup_name=suppliers.name, part_name=parts.name
    )


def bad_cross_7():
    # Reason it is bad: CROSS unused
    return CROSS(regions)


def bad_cross_8():
    # Reason it is bad:  Output column not available in the CROSSed collection
    return regions.CALCULATE(r1=name).CROSS(nations).CALCULATE(r_key, r2=name)


def bad_cross_9():
    # Reason it is bad: Use output of CROSS as an output column
    return regions.CALCULATE(new_col=CROSS(regions))


def bad_cross_10():
    # Reason it is bad: Aggregating on `name` which is a property of regions
    # instead of one of its subcollections.
    return regions.CROSS(regions).CALCULATE(total=COUNT(name))


def bad_cross_11():
    # Reason it is bad: `customers` is a sub-collection of `nations`,
    # not `regions`
    return nations.CROSS(regions).CALCULATE(n=COUNT(customers))


# QUANTILE function's test
# not arguments
def bad_quantile_1():
    return customers.CALCULATE(bad_quantile=QUANTILE(orders.total_price))


# bad arguments
def bad_quantile_2():
    return customers.CALCULATE(bad_quantile=QUANTILE("orders.total_price", 0.7))


# p out of range [0-1]
def bad_quantile_3():
    return customers.CALCULATE(bad_quantile=QUANTILE(orders.total_price, 40))


# negative p out of range [0-1]
def bad_quantile_4():
    return customers.CALCULATE(bad_quantile=QUANTILE(orders.total_price, -10))


# collection
def bad_quantile_5():
    return customers.CALCULATE(bad_quantile=QUANTILE(orders, 0.4))


# both numbers
def bad_quantile_6():
    return customers.CALCULATE(bad_quantile=QUANTILE(20, 0.9))
