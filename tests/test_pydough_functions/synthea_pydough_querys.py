__all__ = [
    "most_common_conditions",
]


# Returns the most common condition for female americans
most_common_conditions = """
result = patients.WHERE(
    (gender == 'F') & (ethnicity == 'american')
).conditions.PARTITION(
    name="condition_groups",
    by=DESCRIPTION
).CALCULATE(
    condition_description=DESCRIPTION,
    occurrence_count=COUNT(conditions)
).TOP_K(
    1,
    by=occurrence_count.DESC()
).CALCULATE(
    condition_description
)
"""
