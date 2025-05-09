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
