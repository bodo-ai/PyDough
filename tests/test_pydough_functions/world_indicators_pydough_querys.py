__all__ = [
    "wdi_albania_footnotes_1981",
    "wdi_low_income_country_with_series",
]

wdi_low_income_country_with_series = """
result = world_development_indicators.Country.WHERE(
    (IncomeGroup == 'Low income') &
    HAS(CountryNotes.WHERE(Series.SeriesCode == 'DT.DOD.DECT.CD'))
).CALCULATE(
    country_code=CountryCode
)
"""

wdi_albania_footnotes_1981 = """
result = world_development_indicators.Country.WHERE(
    ShortName == 'Albania'
).Footnotes.WHERE(
    Year == '1981'
).CALCULATE(
    footnote_description=Description
)
"""
