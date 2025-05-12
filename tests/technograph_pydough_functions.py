"""
Variant of `simple_pydough_functions.py` for the TechnoGraph dataset.
"""

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def global_incident_rate():
    # Return the global rate of incidents per device
    return TechnoGraph.CALCULATE(ir=ROUND(COUNT(incidents) / COUNT(devices), 2))


def incident_rate_per_brand():
    # Return the incident rate for devices of each brand
    device_info = devices.CALCULATE(brand=product.brand, n_incidents=COUNT(incidents))
    return (
        device_info.PARTITION(name="brands", by=brand)
        .CALCULATE(brand, ir=ROUND(SUM(devices.n_incidents) / COUNT(devices), 2))
        .ORDER_BY(brand.ASC())
    )


def most_unreliable_products():
    # Return the 5 most unreliable products, based on the incident rate
    device_info = devices.CALCULATE(brand=product.brand, n_incidents=COUNT(incidents))
    return products.CALCULATE(
        product=name,
        product_brand=brand,
        product_type=category,
        ir=ROUND(SUM(device_info.n_incidents) / COUNT(device_info), 2),
    ).TOP_K(5, by=ir.DESC())


def incident_rate_by_release_year():
    # Return, for each year, the incident rate for products released that year
    device_info = devices.CALCULATE(
        year=YEAR(product.release_date), n_incidents=COUNT(incidents)
    )
    return (
        device_info.PARTITION(name="years", by=year)
        .CALCULATE(year, ir=ROUND(SUM(device_info.n_incidents) / COUNT(device_info), 2))
        .ORDER_BY(year.ASC())
    )


def error_rate_sun_set_by_factory_country():
    # Return the error rate for Sun-Set products, grouped by factory country
    device_info = devices_made.WHERE(product.name == "Sun-Set").CALCULATE(
        n_incidents=COUNT(incidents)
    )
    return countries.CALCULATE(
        country=name, ir=ROUND(SUM(device_info.n_incidents) / COUNT(device_info), 2)
    ).ORDER_BY(country.ASC())


def error_percentages_sun_set_by_error():
    # Return the percentage of errors for Sun-Set products of each error type
    selected_errors = incidents.WHERE(device.product.name == "Sun-Set")
    return errors.CALCULATE(
        error=name,
        pct=ROUND(100.0 * COUNT(selected_errors) / RELSUM(COUNT(selected_errors)), 2),
    ).ORDER_BY(pct.DESC())


def battery_failure_rates_anomalies():
    # Return the 5 product/producing country pairs with the highest battery
    # failure rates.
    return (
        countries.CALCULATE(country_name=name)
        .devices_made.CALCULATE(
            product_name=product.name,
            n_incidents=COUNT(incidents.WHERE(error.name == "Battery Failure")),
        )
        .PARTITION(name="product_manufacturing_pairs", by=(country_name, product_name))
        .CALCULATE(
            country_name,
            product_name,
            ir=ROUND(SUM(devices_made.n_incidents) / COUNT(devices_made), 2),
        )
        .TOP_K(5, by=(ir.DESC(), product_name.ASC(), country_name.ASC()))
    )


def country_incident_rate_analysis():
    # For each country, identify the incident rate of products made in that
    # country, versus the incident rate of products bought in that country,
    # versus the incident rate of products bought by customers from that
    # country.
    return countries.CALCULATE(
        country_name=name,
        made_ir=ROUND(COUNT(devices_made.incidents) / COUNT(devices_made), 2),
        sold_ir=ROUND(COUNT(devices_sold.incidents) / COUNT(devices_sold), 2),
        user_ir=ROUND(COUNT(users.devices.incidents) / COUNT(users.devices), 2),
    ).ORDER_BY(country_name.ASC())


def year_cumulative_incident_rate_goldcopperstar():
    # Break the cumulative incident rate for GoldCopper-Star devices down
    # by the years since the product was released, and also include the
    # percent change from the previous year in the number of incidents versus
    # the number of devices purchased, and the number of purchases/incidents.
    # from that year.
    years = calendar.CALCULATE(year=YEAR(calendar_day)).PARTITION(name="years", by=year)
    p_filter = name == "GoldCopper-Star"
    selected_devices_product = calendar.devices_sold.product.WHERE(p_filter)
    return (
        years.CALCULATE(
            release_date=ANYTHING(selected_devices_product.release_date),
            n_devices=COUNT(selected_devices_product),
            n_incidents=COUNT(calendar.incidents_reported.WHERE(p_filter)),
        )
        .WHERE(YEAR(release_date) <= year)
        .CALCULATE(
            years_since_release=year - YEAR(release_date),
            cum_ir=ROUND(
                RELSUM(n_incidents, by=year.ASC(), cumulative=True)
                / RELSUM(n_devices, by=year.ASC(), cumulative=True),
                2,
            ),
            pct_bought_change=ROUND(
                (100.0 * (n_devices - PREV(n_devices, by=year.ASC())))
                / PREV(n_devices, by=year.ASC()),
                2,
            ),
            pct_incident_change=ROUND(
                (100.0 * (n_incidents - PREV(n_incidents, by=year.ASC())))
                / PREV(n_incidents, by=year.ASC()),
                2,
            ),
            bought=n_devices,
            incidents=n_incidents,
        )
        .ORDER_BY(years_since_release.ASC())
    )


def year_cumulative_incident_rate_overall():
    # Break the cumulative incident rate for ALL devices down
    # by the the year and also include the percent change from the
    # previous year in the number of incidents versus the raw number
    # of devices purchased, and the number of purchases/incidents.
    # from that year.
    years = calendar.CALCULATE(year=YEAR(calendar_day)).PARTITION(name="years", by=year)
    return (
        years.CALCULATE(
            n_devices=COUNT(calendar.devices_sold),
            n_incidents=COUNT(calendar.incidents_reported),
        )
        .WHERE(n_devices > 0)
        .CALCULATE(
            yr=year,
            cum_ir=ROUND(
                RELSUM(n_incidents, by=year.ASC(), cumulative=True)
                / RELSUM(n_devices, by=year.ASC(), cumulative=True),
                2,
            ),
            pct_bought_change=ROUND(
                (100.0 * (n_devices - PREV(n_devices, by=year.ASC())))
                / PREV(n_devices, by=year.ASC()),
                2,
            ),
            pct_incident_change=ROUND(
                (100.0 * (n_incidents - PREV(n_incidents, by=year.ASC())))
                / PREV(n_incidents, by=year.ASC()),
                2,
            ),
            bought=n_devices,
            incidents=n_incidents,
        )
        .ORDER_BY(yr.ASC())
    )
