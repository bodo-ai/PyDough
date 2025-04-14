__all__ = [
    "events_per_season",
    "first_event_per_era",
    "summer_events_per_type",
]

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def first_event_per_era():
    # Returns the first event per era, with the event name, sorted
    # based on the chronological order of the eras.
    first_event = events.WHERE(RANKING(by=date_time.ASC(), per="eras") == 1).SINGULAR()
    return eras.CALCULATE(
        era_name=name,
        event_name=first_event.name,
    ).ORDER_BY(start_year.ASC())


def events_per_season():
    # Returns the total number of events per season.
    return seasons.CALCULATE(season_name=name, n_events=COUNT(events)).ORDER_BY(
        n_events.DESC(), season_name.ASC()
    )


def events_per_season():
    # Returns the total number of events per season.
    return seasons.CALCULATE(season_name=name, n_events=COUNT(events)).ORDER_BY(
        n_events.DESC(), season_name.ASC()
    )


def summer_events_per_type():
    # Counts how many events happened in the summer for each event type.
    return (
        seasons.WHERE(name == "Summer")
        .events.PARTITION(name="types", by=event_type)
        .CALCULATE(event_type, n_events=COUNT(events))
        .ORDER_BY(event_type.ASC())
    )


def num_predawn_cold_war():
    # Counts how many events happened in the in the pre-dawn hours of the
    # cold war.
    selected_events = events.WHERE(HAS(time_of_day.WHERE(name == "Pre-Dawn"))).WHERE(
        HAS(era.WHERE(name == "Cold War"))
    )
    return Epoch.CALCULATE(n_events=COUNT(selected_events))


def num_predawn_cold_war():
    # Counts how many events happened in the in the pre-dawn hours of the
    # cold war.
    selected_events = events.WHERE(HAS(time_of_day.WHERE(name == "Pre-Dawn"))).WHERE(
        HAS(era.WHERE(name == "Cold War"))
    )
    return Epoch.CALCULATE(n_events=COUNT(selected_events))


def culture_events_info():
    # Finds the first 6 cultural events and lists their name, era, year,
    # season, and time of day, ordered chronologically by event.
    return (
        events.WHERE(event_type == "culture")
        .CALCULATE(
            event_name=name,
            era_name=era.name,
            event_year=YEAR(date_time),
            season_name=season.name,
            tod=time_of_day.name,
        )
        .TOP_K(6, by=date_time.ASC())
    )


"""
SELECT ev_name, STRFTIME('%Y', ev_dt), er_name, t_name, s_name
FROM ERAS, EVENTS, TIMES, SEASONS
WHERE CAST(STRFTIME('%Y', ev_dt) AS INTEGER) >= er_start_year
AND CAST(STRFTIME('%Y', ev_dt) AS INTEGER) < er_end_year
AND CAST(STRFTIME('%H', ev_dt) AS INTEGER) >= t_start_hour
AND CAST(STRFTIME('%H', ev_dt) AS INTEGER) < t_end_hour
AND CAST(STRFTIME('%m', ev_dt) AS INTEGER) IN (s_first_month, s_second_month, s_third_month)
AND ev_typ = 'culture'
ORDER BY ev_dt ASC
LIMIT 6;
"""
