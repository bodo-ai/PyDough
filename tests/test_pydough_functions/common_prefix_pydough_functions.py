"""
Various functions containing PyDough code snippets for testing purposes.
Specifically, tests for various edge cases involving common prefixes of logic
being syncretized to avoid duplicate logic.
"""
# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def common_prefix_a():
    # For each region, count how many nations & customers are in the region.
    return regions.CALCULATE(
        name,
        n_nations=COUNT(nations),
        n_customers=COUNT(nations.customers),
    ).ORDER_BY(name.ASC())


def common_prefix_b():
    # For each region, count how many nations, customers and suppliers are in
    # the region.
    return regions.CALCULATE(
        name,
        n_nations=COUNT(nations),
        n_customers=COUNT(nations.customers),
        n_suppliers=COUNT(nations.suppliers),
    ).ORDER_BY(name.ASC())


def common_prefix_c():
    # For each region, count how many nations, customers and suppliers are in
    # the region, as well as how many orders were made by customers in that
    # region and how many parts are supplied by suppliers in that region.
    return regions.CALCULATE(
        name,
        n_nations=COUNT(nations),
        n_customers=COUNT(nations.customers),
        n_suppliers=COUNT(nations.suppliers),
        n_orders=COUNT(nations.customers.orders),
        n_parts=COUNT(nations.suppliers.supply_records),
    ).ORDER_BY(name.ASC())


def common_prefix_d():
    # For each region, count how many nations, customers and suppliers are in
    # the region, as well as how many orders were made by customers in that
    # region in 1994, 1995 and 1996.
    return regions.CALCULATE(
        name,
        n_nations=COUNT(nations),
        n_customers=COUNT(nations.customers),
        n_suppliers=COUNT(nations.suppliers),
        n_orders_94=COUNT(nations.customers.orders.WHERE(YEAR(order_date) == 1994)),
        n_orders_95=COUNT(nations.customers.orders.WHERE(YEAR(order_date) == 1995)),
        n_orders_96=COUNT(nations.customers.orders.WHERE(YEAR(order_date) == 1996)),
    ).ORDER_BY(name.ASC())


def common_prefix_e():
    # Same as common_prefix_a, but a different order of the fields.
    return (
        regions.CALCULATE(
            n_customers=COUNT(nations.customers),
        )
        .CALCULATE(
            name,
            n_customers,
            n_nations=COUNT(nations),
        )
        .ORDER_BY(name.ASC())
    )


def common_prefix_f():
    # Same as common_prefix_b, but a different order of the fields.
    return (
        regions.CALCULATE(
            n_customers=COUNT(nations.customers),
        )
        .CALCULATE(
            n_nations=COUNT(nations),
        )
        .CALCULATE(
            name,
            n_customers,
            n_nations,
            n_suppliers=COUNT(nations.suppliers),
        )
        .ORDER_BY(name.ASC())
    )


def common_prefix_g():
    # Same as common_prefix_b, but a different order of the fields.
    return (
        regions.CALCULATE(
            n_customers=COUNT(nations.customers),
            n_suppliers=COUNT(nations.suppliers),
        )
        .CALCULATE(
            name,
            n_customers,
            n_suppliers,
            n_nations=COUNT(nations),
        )
        .ORDER_BY(name.ASC())
    )


def common_prefix_h():
    # Same as common_prefix_c, but a different order of the fields.
    return (
        regions.CALCULATE(
            n_nations=COUNT(nations),
            n_orders=COUNT(nations.customers.orders),
        )
        .CALCULATE(
            n_customers=COUNT(nations.customers),
            n_parts=COUNT(nations.suppliers.supply_records),
        )
        .CALCULATE(
            name,
            n_nations,
            n_orders,
            n_customers,
            n_parts,
            n_suppliers=COUNT(nations.suppliers),
        )
        .ORDER_BY(name.ASC())
    )


def common_prefix_i():
    # For each nation, count how many customers it has, and how many orders
    # were made by customers in that nation in December of 1992 through clerk
    # 272. Does NOT preserve any nations without any such orders. Keeps the
    # 5 kept nations with the most total customers.
    selected_orders = customers.orders.WHERE(
        (YEAR(order_date) == 1992)
        & (MONTH(order_date) == 12)
        & (clerk == "Clerk#000000272")
    )
    return (
        nations.CALCULATE(
            name,
            n_customers=COUNT(customers),
            n_selected_orders=COUNT(selected_orders),
        )
        .WHERE(HAS(selected_orders))
        .TOP_K(5, by=(n_customers.DESC(), name.ASC()))
    )


def common_prefix_j():
    # For each customer, get its nation name and region name, and pick the
    # first 5 customers alphabetically.
    return customers.CALCULATE(
        cust_name=name,
        nation_name=nation.name,
        region_name=nation.region.name,
    ).TOP_K(5, by=name.ASC())


def common_prefix_k():
    # Same as common_prefix_j, but a different order of the fields.
    return (
        customers.CALCULATE(
            region_name=nation.region.name,
        )
        .CALCULATE(
            cust_name=name,
            region_name=region_name,
            nation_name=nation.name,
        )
        .TOP_K(5, by=name.ASC())
    )


def common_prefix_l():
    # For each european customer, get its nation name and count the number of
    # suppliers in the same nation who supply at least 5 mint products, as well
    # as the minimum, maximum, average and total account balance of all such
    # suppliers, and pick the first 5 customers alphabetically.
    mint_part = supply_records.part.WHERE(CONTAINS(name, "mint"))
    selected_suppliers = nation.suppliers.WHERE(COUNT(mint_part) >= 5)
    selected_customers = customers.WHERE(nation.region.name == "EUROPE").TOP_K(
        5, by=name.ASC()
    )
    return selected_customers.CALCULATE(
        cust_name=name,
        nation_name=nation.name,
        n_selected_suppliers=COUNT(selected_suppliers),
        selected_suppliers_min=MIN(selected_suppliers.account_balance),
        selected_suppliers_max=MAX(selected_suppliers.account_balance),
        selected_suppliers_avg=ROUND(AVG(selected_suppliers.account_balance), 2),
        selected_suppliers_sum=SUM(selected_suppliers.account_balance),
    )


def common_prefix_m():
    # Same as common_prefix_l, but a different order of the fields.
    mint_part = supply_records.part.WHERE(CONTAINS(name, "mint"))
    selected_suppliers = nation.suppliers.WHERE(COUNT(mint_part) >= 5)
    selected_customers = customers
    return (
        selected_customers.CALCULATE(
            cust_name=name,
            n_selected_suppliers=COUNT(selected_suppliers),
            selected_suppliers_min=MIN(selected_suppliers.account_balance),
            selected_suppliers_max=MAX(selected_suppliers.account_balance),
            selected_suppliers_avg=ROUND(AVG(selected_suppliers.account_balance), 2),
            selected_suppliers_sum=SUM(selected_suppliers.account_balance),
        )
        .CALCULATE(
            cust_name,
            n_selected_suppliers,
            selected_suppliers_min,
            selected_suppliers_max,
            selected_suppliers_avg,
            selected_suppliers_sum,
            nation_name=nation.name,
        )
        .WHERE(nation.region.name == "EUROPE")
        .TOP_K(5, by=name.ASC())
    )


def common_prefix_n():
    # For each order, get the number of elements in the order that were
    # shipped in 1996, and for those elements the the total retail price of al
    # parts ordered the number of distinct nation that supplied the orders,
    # the maximum account balance of any of the order's suppliers, and the
    # elements that were for a small part. Only consider orders where there is at
    # least one duplicate supplier nation, and pick the five most recent
    # qualifying orders, breaking ties by the key. Only consider orders
    selected_lines = lines.WHERE((YEAR(ship_date) == 1996) & (MONTH(ship_date) == 11))
    return (
        orders.WHERE((YEAR(order_date) == 1996) & (QUARTER(order_date) == 4))
        .CALCULATE(
            key,
            order_date,
            n_elements=COUNT(selected_lines),
            total_retail_price=SUM(selected_lines.part.retail_price),
            n_unique_supplier_nations=NDISTINCT(selected_lines.supplier.nation.name),
            max_supplier_balance=MAX(selected_lines.supplier.account_balance),
            n_small_parts=COUNT(selected_lines.part.WHERE(STARTSWITH(container, "SM"))),
        )
        .WHERE(n_elements > n_unique_supplier_nations)
        .TOP_K(5, by=(order_date.DESC(), key.ASC()))
    )


def common_prefix_o():
    # Same as common_prefix_n, but only allowing orders with at least
    # 1 small part
    selected_lines = lines.WHERE((YEAR(ship_date) == 1996) & (MONTH(ship_date) == 11))
    small_parts = selected_lines.part.WHERE(STARTSWITH(container, "SM"))
    return (
        orders.WHERE((YEAR(order_date) == 1996) & (QUARTER(order_date) == 4))
        .WHERE(HAS(small_parts))
        .CALCULATE(
            key,
            order_date,
            n_elements=COUNT(selected_lines),
            total_retail_price=SUM(selected_lines.part.retail_price),
            n_unique_supplier_nations=NDISTINCT(selected_lines.supplier.nation.name),
            max_supplier_balance=MAX(selected_lines.supplier.account_balance),
            n_small_parts=COUNT(small_parts),
        )
        .WHERE(n_elements > n_unique_supplier_nations)
        .TOP_K(5, by=(order_date.DESC(), key.ASC()))
    )


def common_prefix_p():
    # For each customer, count how many orders they have made, the number of
    # qualifying lineitems within those orders, and the number of unique parts
    # they have ordered, keeping the top 5 customers with the smallest ratio of
    # unique parts to total part orders, breaking ties by customer
    # name. Only consider urgent orders, and lineitems without a tax.
    selected_orders = orders.WHERE(order_priority == "1-URGENT")
    selected_lines = selected_orders.lines.WHERE(tax == 0)
    return (
        customers.CALCULATE(
            name,
            n_orders=COUNT(selected_orders),
            n_parts_ordered=COUNT(selected_lines),
            n_distinct_parts=NDISTINCT(selected_lines.part_key),
        )
        .WHERE(HAS(selected_orders) & HAS(selected_lines))
        .TOP_K(5, by=((n_distinct_parts / n_parts_ordered).ASC(), name.ASC()))
    )


def common_prefix_q():
    # For each customer, obtain the total price they have paid for all urgent
    # orders in 1998, identify the single most expensive line item from any
    # of those orders (breaking ties by part key) and the part name & extended
    # price of that line item. Pick the 5 customers with the highest total
    # price paid, breaking ties by customer name.
    selected_orders = orders.WHERE(
        (order_priority == "1-URGENT") & (YEAR(order_date) == 1998)
    )
    most_expensive_line = selected_orders.lines.BEST(
        by=(extended_price.DESC(), part_key.ASC()), per="customers"
    )
    return customers.CALCULATE(
        name,
        total_spent=SUM(selected_orders.total_price),
        line_price=most_expensive_line.extended_price,
        part_name=most_expensive_line.part.name,
    ).TOP_K(5, by=(total_spent.DESC(), name.ASC()))


def common_prefix_r():
    # Same as common_prefix_q, but with HAS filters and a different order of
    # the fields.
    selected_orders = orders.WHERE(
        (order_priority == "1-URGENT") & (YEAR(order_date) == 1998)
    )
    most_expensive_line = selected_orders.lines.BEST(
        by=(extended_price.DESC(), part_key.ASC()), per="customers"
    )
    return (
        customers.CALCULATE(part_name=most_expensive_line.part.name)
        .CALCULATE(line_price=most_expensive_line.extended_price)
        .CALCULATE(
            name,
            part_name,
            line_price,
            total_spent=SUM(selected_orders.total_price),
        )
        .WHERE(HAS(selected_orders) & HAS(most_expensive_line))
        .TOP_K(5, by=(total_spent.DESC(), name.ASC()))
    )


def common_prefix_s():
    # For each german customer in the automobile industry, obtain the date of
    # their most recent order (breaking ties by order key) and how many total
    # vs unique suppliers were in that order. Keep all such customers who had
    # at least one duplicate supplier in their order, sorted by customer name.
    most_recent_order = orders.WHERE(YEAR(order_date) == 1998).BEST(
        by=(order_date.DESC(), key.ASC()), per="customers"
    )
    selected_lines = most_recent_order.lines.WHERE(YEAR(ship_date) == 1998)
    return (
        customers.WHERE((nation.name == "GERMANY") & (market_segment == "AUTOMOBILE"))
        .CALCULATE(
            name,
            most_recent_order_date=most_recent_order.order_date,
            most_recent_order_total=COUNT(selected_lines),
            most_recent_order_distinct=NDISTINCT(selected_lines.supplier_key),
        )
        .WHERE(most_recent_order_distinct < most_recent_order_total)
        .ORDER_BY(name.ASC())
    )


def common_prefix_t():
    # For each Indian customer in the building industry who has orders, get the
    # total sum of quantities of parts they have ordered, picking the 5
    # customers with the highest quantities (breaking ties by name).
    return (
        customers.WHERE(
            (nation.name == "INDIA") & (market_segment == "BUILDING") & HAS(orders)
        )
        .CALCULATE(
            name,
            total_qty=SUM(orders.lines.quantity),
        )
        .WHERE(HAS(orders))
        .TOP_K(5, by=(total_qty.DESC(), name.ASC()))
    )


def common_prefix_u():
    # Same as common_prefix_t but only considers lineitems with a tax of 0
    # shipped via rail, and ignores customers without any such orders.
    selected_lines = orders.lines.WHERE((tax == 0) & (ship_mode == "RAIL"))
    return (
        customers.WHERE(
            (nation.name == "INDIA") & (market_segment == "BUILDING") & HAS(orders)
        )
        .CALCULATE(
            name,
            total_qty=SUM(selected_lines.quantity),
        )
        .WHERE(HAS(orders) & HAS(selected_lines))
        .TOP_K(5, by=(total_qty.DESC(), name.ASC()))
    )


def common_prefix_v():
    # For each customer whose nation name starts with "A" get their region's
    # name, and keep the first 5 such customers alphabetically by name.
    selected_nation = nation.WHERE(name[:1] == "A")
    return (
        customers.WHERE(HAS(selected_nation))
        .CALCULATE(name, region_name=selected_nation.region.name)
        .TOP_K(5, by=name.ASC())
    )


def common_prefix_w():
    # For each order purchasing customer is not in debt and the customer's
    # nation starts with "A", get the order key and customer's nation's name
    # of the first 5 qualifying orders with the lowest key.
    selected_customer = customer.WHERE(account_balance > 0.0)
    selected_nation = selected_customer.nation.WHERE(name[:1] == "A")
    return (
        orders.WHERE(HAS(selected_customer) & HAS(selected_nation))
        .CALCULATE(key, cust_nation_name=selected_nation.name)
        .TOP_K(5, by=key.ASC())
    )


def common_prefix_x():
    # For each customer who has made a zero-tax purchase, count how
    # many total orders they have made. Keep the top 5 customers by number
    # of orders, breaking ties by customer name.
    return (
        customers.WHERE(HAS(orders.lines.WHERE(tax == 0)))
        .CALCULATE(name, n_orders=COUNT(orders))
        .TOP_K(5, by=(n_orders.DESC(), name.ASC()))
    )


def common_prefix_y():
    # For each customer who has NEVER made a zero-tax purchase through clerk
    # number 1, count how many total orders they have made through that clerk.
    # Keep the top 5 customers by number of orders, breaking ties by customer
    # name.
    clerk_one_orders = orders.WHERE(clerk == "Clerk#000000001")
    return (
        customers.WHERE(HASNOT(clerk_one_orders.lines.WHERE(tax == 0)))
        .CALCULATE(name, n_orders=COUNT(clerk_one_orders))
        .TOP_K(5, by=(n_orders.DESC(), name.ASC()))
    )


def common_prefix_z():
    # For each Asian customer, get the name of the country they live in.
    # Keep the first 5 qualifying customers alphabetically.
    return (
        customers.WHERE(HAS(nation.region.WHERE(name == "ASIA")))
        .CALCULATE(
            name,
            nation_name=nation.name,
        )
        .TOP_K(5, by=(name.ASC()))
    )


def common_prefix_aa():
    # For each non-American customer, get the name of the country they live in.
    # Keep the first 5 qualifying customers alphabetically.
    return (
        customers.WHERE(HASNOT(nation.region.WHERE(name == "AMERICA")))
        .CALCULATE(
            name,
            nation_name=nation.name,
        )
        .TOP_K(5, by=(name.ASC()))
    )


def common_prefix_ab():
    # Count how many orders were made by a customer not in debt from Japan.
    selected_customer = customer.WHERE(account_balance > 0.0)
    selected_nation = selected_customer.nation.WHERE(name == "JAPAN")
    selected_orders = orders.WHERE(HAS(selected_customer) & HAS(selected_nation))
    return TPCH.CALCULATE(n=COUNT(selected_orders))


def common_prefix_ac():
    # Count how many orders were NOT made by a customer not in debt, NOR
    # from a customer in debt from Japan.
    selected_customer = customer.WHERE(account_balance > 0.0)
    selected_nation = selected_customer.nation.WHERE(name == "JAPAN")
    selected_orders = orders.WHERE(HASNOT(selected_customer) & HASNOT(selected_nation))
    return TPCH.CALCULATE(n=COUNT(selected_orders))


def common_prefix_ad():
    # For each Japanese supplier find the part they sell with the highest
    # available quantity that has a "WRAP CASE" container (breaking ties
    # alphabetically by part name), and the total quantity of the part shipped
    # by the supplier in the first 3 days of February of 1995. Only consider
    # suppliers who had any of that part shipped on that date, ordered
    # alphabetically.
    best_supply_record = supply_records.WHERE(part.container == "WRAP CASE").BEST(
        by=(available_quantity.DESC(), part.name.ASC()), per="suppliers"
    )
    selected_lines = best_supply_record.lines.WHERE(
        (YEAR(ship_date) == 1995) & (MONTH(ship_date) == 2) & (DAY(ship_date) < 4)
    )
    selected_suppliers = suppliers.WHERE((nation.name == "JAPAN") & HAS(selected_lines))
    return selected_suppliers.CALCULATE(
        supplier_name=name,
        part_name=best_supply_record.part.name,
        part_qty=best_supply_record.available_quantity,
        qty_shipped=SUM(selected_lines.quantity),
    ).ORDER_BY(name.ASC())


def common_prefix_ae():
    # For each Asian nation, count how many customers are in that nation, and
    # the name of the customers who made orders 1070368, 1347104, 1472135, or
    # 2351457 (each of those orders is from a different nation).
    selected_orders = customers.orders.WHERE(
        ISIN(key, (1070368, 1347104, 1472135, 2351457))
    ).SINGULAR()
    return (
        nations.WHERE(region.name == "ASIA")
        .CALCULATE(
            nation_name=name,
            n_customers=COUNT(customers),
            customer_name=selected_orders.customer.name,
        )
        .ORDER_BY(nation_name.ASC())
    )


def common_prefix_af():
    # Same as common_prefix_ae, but ignores nations without one of the
    # requested customers/orders.
    selected_orders = customers.orders.WHERE(
        ISIN(key, (1070368, 1347104, 1472135, 2351457))
    ).SINGULAR()
    return (
        nations.WHERE((region.name == "ASIA") & HAS(selected_orders))
        .CALCULATE(
            nation_name=name,
            n_customers=COUNT(customers),
            customer_name=selected_orders.customer.name,
        )
        .ORDER_BY(nation_name.ASC())
    )


def common_prefix_ag():
    # For every european nation, count how many customers in that region are in
    # the machinery market segment, how many orders were made in 1998 by those
    # customers had priority 2-HIGH, how many line items were in those orders
    # that were shipped by a supplier in the same nation via truck, and the
    # total revenue from those line items. The revenue accounts for the
    # discount, and the cost to the supplier. The result is sorted
    # alphabetically by nation name. Assume such lineitems will always exist
    # for each nation.
    selected_customers = customers.WHERE(market_segment == "MACHINERY")
    selected_orders = selected_customers.orders.WHERE(
        (order_priority == "2-HIGH") & (YEAR(order_date) == 1998)
    )

    selected_lines = (
        selected_orders.lines.WHERE(ship_mode == "TRUCK")
        .WHERE(supplier.nation.name == nation_name)
        .CALCULATE(
            revenue=extended_price * (1 - discount)
            - quantity * part_and_supplier.supply_cost
        )
    )
    return (
        nations.WHERE(region.name == "EUROPE")
        .CALCULATE(nation_name=name)
        .CALCULATE(
            nation_name,
            n_machine_cust=COUNT(selected_customers),
            n_machine_high_orders=COUNT(selected_orders),
            n_machine_high_domestic_lines=COUNT(selected_lines),
            total_machine_high_domestic_revenue=ROUND(SUM(selected_lines.revenue), 2),
        )
        .WHERE(HAS(selected_lines))
        .ORDER_BY(nation_name.ASC())
    )


def common_prefix_ah():
    # Same as common_prefix_ag, but with a different subset of fields.
    selected_customers = customers.WHERE(market_segment == "MACHINERY")
    selected_orders = selected_customers.orders.WHERE(
        (order_priority == "2-HIGH") & (YEAR(order_date) == 1998)
    )

    selected_lines = (
        selected_orders.lines.WHERE(ship_mode == "TRUCK")
        .WHERE(supplier.nation.name == nation_name)
        .CALCULATE(
            revenue=extended_price * (1 - discount)
            - quantity * part_and_supplier.supply_cost
        )
    )
    return (
        nations.WHERE(region.name == "EUROPE")
        .CALCULATE(nation_name=name)
        .CALCULATE(
            nation_name,
            n_machine_high_orders=COUNT(selected_orders),
            n_machine_high_domestic_lines=COUNT(selected_lines),
            total_machine_high_domestic_revenue=ROUND(SUM(selected_lines.revenue), 2),
        )
        .WHERE(HAS(selected_lines))
        .ORDER_BY(nation_name.ASC())
    )


def common_prefix_ai():
    # Same as common_prefix_ag, but with a different subset of fields.
    selected_customers = customers.WHERE(market_segment == "MACHINERY")
    selected_orders = selected_customers.orders.WHERE(
        (order_priority == "2-HIGH") & (YEAR(order_date) == 1998)
    )

    selected_lines = (
        selected_orders.lines.WHERE(ship_mode == "TRUCK")
        .WHERE(supplier.nation.name == nation_name)
        .CALCULATE(
            revenue=extended_price * (1 - discount)
            - quantity * part_and_supplier.supply_cost
        )
    )
    return (
        nations.WHERE(region.name == "EUROPE")
        .CALCULATE(nation_name=name)
        .CALCULATE(
            nation_name,
            n_machine_cust=COUNT(selected_customers),
            n_machine_high_domestic_lines=COUNT(selected_lines),
            total_machine_high_domestic_revenue=ROUND(SUM(selected_lines.revenue), 2),
        )
        .WHERE(HAS(selected_lines))
        .ORDER_BY(nation_name.ASC())
    )


def common_prefix_aj():
    # Same as common_prefix_ag, but with a different subset of fields.
    selected_customers = customers.WHERE(market_segment == "MACHINERY")
    selected_orders = selected_customers.orders.WHERE(
        (order_priority == "2-HIGH") & (YEAR(order_date) == 1998)
    )
    selected_lines = (
        selected_orders.lines.WHERE(ship_mode == "TRUCK")
        .WHERE(supplier.nation.name == nation_name)
        .CALCULATE(
            revenue=extended_price * (1 - discount)
            - quantity * part_and_supplier.supply_cost
        )
    )
    return (
        nations.WHERE(region.name == "EUROPE")
        .CALCULATE(nation_name=name)
        .CALCULATE(
            nation_name,
            n_machine_cust=COUNT(selected_customers),
            n_machine_high_orders=COUNT(selected_orders),
            total_machine_high_domestic_revenue=ROUND(SUM(selected_lines.revenue), 2),
        )
        .WHERE(HAS(selected_lines))
        .ORDER_BY(nation_name.ASC())
    )


def common_prefix_ak():
    # Same as common_prefix_ag, but with a different subset of fields.
    selected_customers = customers.WHERE(market_segment == "MACHINERY")
    selected_orders = selected_customers.orders.WHERE(
        (order_priority == "2-HIGH") & (YEAR(order_date) == 1998)
    )
    selected_lines = (
        selected_orders.lines.WHERE(ship_mode == "TRUCK")
        .WHERE(supplier.nation.name == nation_name)
        .CALCULATE(
            revenue=extended_price * (1 - discount)
            - quantity * part_and_supplier.supply_cost
        )
    )
    return (
        nations.WHERE(region.name == "EUROPE")
        .CALCULATE(nation_name=name)
        .CALCULATE(
            nation_name,
            n_machine_cust=COUNT(selected_customers),
            n_machine_high_orders=COUNT(selected_orders),
            n_machine_high_domestic_lines=COUNT(selected_lines),
        )
        .WHERE(HAS(selected_lines))
        .ORDER_BY(nation_name.ASC())
    )


def common_prefix_al():
    # Select the top 10 customers whose number of orders made is above the
    # average for all numbers of orders made by any customer in the nation,
    # and who have made at least one purchase without a discount or tax.
    # Filter those top 10 to only include customers who have made at least
    # one purchase of a part with size below 15 (also without tax or discount).
    # For each remaining customer, list their key, number of orders made, and
    # number of lineitems without tax/discount made. When choosing the top 10
    # customers, pick the 10 with the lowest key values.
    selected_lines = orders.lines.WHERE(
        (tax == 0)
        & (discount == 0)
        & ISIN(
            part_key,
            (
                53360,
                123069,
                132776,
                62217,
                67393,
                87784,
                148252,
                176947,
                196620,
                103099,
                169275,
            ),
        )
    )
    selected_part_purchase = selected_lines.part.WHERE(size < 15)
    return (
        nations.customers.CALCULATE(n_orders=COUNT(orders))
        .WHERE(n_orders > RELAVG(n_orders, per="nations"))
        .CALCULATE(n_no_tax_discount=COUNT(selected_lines))
        .WHERE(HAS(selected_lines))
        .TOP_K(10, by=key.ASC())
        .CALCULATE(cust_key=key, n_orders=n_orders, n_no_tax_discount=n_no_tax_discount)
        .WHERE(HAS(selected_part_purchase) & (COUNT(selected_part_purchase) != 0))
    )


def common_prefix_am():
    # Same as common_prefix_al, but pick the top 10 of customers with above
    # average number of orders per nation before filtering on whether they have
    # any lineitems without tax/discount.
    selected_lines = orders.lines.WHERE((tax == 0) & (discount == 0))
    selected_part_purchase = selected_lines.part.WHERE(size < 15)
    return (
        nations.customers.CALCULATE(n_orders=COUNT(orders))
        .WHERE(n_orders > RELAVG(n_orders, per="nations"))
        .TOP_K(10, by=key.ASC())
        .WHERE(
            HAS(selected_lines)
            & HAS(selected_part_purchase)
            & (COUNT(selected_part_purchase) != 0)
        )
        .CALCULATE(
            cust_key=key, n_orders=n_orders, n_no_tax_discount=COUNT(selected_lines)
        )
    )


def common_prefix_an():
    # Same as common_prefix_al, but pick the top 50 of customers with any
    # qualifying lineitems before filtering on whether they have an above
    # average number of orders.
    selected_lines = orders.lines.WHERE((tax == 0) & (discount == 0))
    selected_part_purchase = selected_lines.part.WHERE(size < 15)
    return (
        nations.customers.WHERE(HAS(selected_lines))
        .TOP_K(50, by=key.ASC())
        .WHERE(
            (COUNT(orders) > RELAVG(COUNT(orders), per="nations"))
            & HAS(selected_part_purchase)
            & (COUNT(selected_part_purchase) != 0)
        )
        .CALCULATE(
            cust_key=key,
            n_orders=COUNT(orders),
            n_no_tax_discount=COUNT(selected_lines),
        )
    )


def common_prefix_ao():
    # Same as common_prefix_al, but pick the top 20 of customers with any
    # qualifying part purchases before filtering on whether they have an above
    # average number of orders. Also filters to only include customers from
    # france from the BUILDING market segment, and adjust the part filter to
    # only include parts with size below 5 instead of 15, then at the end
    # only keep the top 5 remaining customers alphabetically. For the sake of
    # sanity, pre-trims the french building customers to only the first 35
    # by key before filtering on whether they have any qualifying part
    # purchases.
    selected_lines = orders.lines.WHERE((tax == 0) & (discount == 0))
    return (
        customers.WHERE((nation.name == "FRANCE") & (market_segment == "BUILDING"))
        .TOP_K(35, by=key.ASC())
        .WHERE(HAS(selected_lines.part.WHERE(size < 5)))
        .CALCULATE(n_part_purchases=COUNT(selected_lines.part.WHERE(size < 5)))
        .TOP_K(20, by=key.ASC())
        .WHERE((COUNT(orders) > RELAVG(COUNT(orders))) & HAS(selected_lines))
        .CALCULATE(
            cust_key=key,
            n_orders=COUNT(orders),
            n_no_tax_discount=COUNT(selected_lines),
            n_part_purchases=n_part_purchases,
        )
        .TOP_K(5, by=key.ASC())
    )


def common_prefix_ap():
    # For every size-10 pink part from brand 32, identify the name of the
    # supplier with the greatest available quantity of the part, how much of
    # the part they have, and the nation that supplier is from. Sort the
    # results alphabetically by part.
    best_record = supply_records.BEST(by=available_quantity.DESC(), per="parts")
    return (
        parts.WHERE(CONTAINS(name, "pink") & (brand == "Brand#32") & (size == 10))
        .CALCULATE(
            part_name=name,
            supplier_name=best_record.supplier.name,
            supplier_quantity=best_record.available_quantity,
            supplier_nation=best_record.supplier.nation.name,
        )
        .ORDER_BY(part_name.ASC())
    )


def common_prefix_aq():
    # For every region, identify the nation with the first name alphabetically,
    # the supplier within that nation with the highest account balance, and the
    # part with the highest available quantity supplied by that supplier.
    # Return the region name, nation name, supplier name, part name, and the
    # quantity of the part supplied by that supplier. Sort the results
    # alphabetically by region name.
    best_nation = nations.BEST(by=name.ASC(), per="regions")
    best_supplier = best_nation.suppliers.BEST(by=account_balance.DESC(), per="nations")
    best_record = best_supplier.supply_records.BEST(
        by=available_quantity.DESC(), per="suppliers"
    )
    return regions.CALCULATE(
        region_name=name,
        nation_name=best_nation.name,
        best_supplier=best_supplier.name,
        best_part=best_record.part.name,
        best_quantity=best_record.available_quantity,
    ).ORDER_BY(name.ASC())
