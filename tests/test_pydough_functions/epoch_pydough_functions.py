__all__ = [
    "culture_events_info",
    "event_gap_per_era",
    "events_per_season",
    "first_event_per_era",
    "intra_season_searches",
    "most_popular_search_engine_per_tod",
    "most_popular_topic_per_region",
    "num_predawn_cold_war",
    "pct_searches_per_tod",
    "summer_events_per_type",
    "unique_users_per_engine",
    "users_most_cold_war_searches",
]

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def first_event_per_era():
    # Returns the first event per era, with the event name, sorted
    # based on the chronological order of the eras.
    first_event = events.BEST(by=date_time.ASC(), per="eras")
    return (
        eras.WHERE(HAS(first_event))
        .CALCULATE(
            era_name=name,
            event_name=first_event.name,
        )
        .ORDER_BY(start_year.ASC())
    )


def events_per_season():
    # Returns the total number of events per season.
    return seasons.CALCULATE(season_name=name, n_events=COUNT(events)).ORDER_BY(
        n_events.DESC(), season_name.ASC()
    )


def summer_events_per_type():
    # Counts how many events happened in the summer for each event type.
    return (
        events.WHERE(season.name == "Summer")
        .PARTITION(name="types", by=event_type)
        .CALCULATE(event_type, n_events=COUNT(events))
        .ORDER_BY(event_type.ASC())
    )


def num_predawn_cold_war():
    # Counts how many events happened in the in the pre-dawn hours of the
    # cold war.
    selected_events = events.WHERE(
        HAS(time_of_day.WHERE(name == "Pre-Dawn")) & HAS(era.WHERE(name == "Cold War"))
    )
    return Epoch.CALCULATE(n_events=COUNT(selected_events))


def culture_events_info():
    # Finds the first 6 cultural events and lists their name, era, year,
    # season, and time of day, ordered chronologically by event.
    return (
        events.WHERE(
            (event_type == "culture") & HAS(era) & HAS(season) & HAS(time_of_day)
        )
        .CALCULATE(
            event_name=name,
            era_name=era.name,
            event_year=YEAR(date_time),
            season_name=season.name,
            tod=time_of_day.name,
        )
        .TOP_K(6, by=date_time.ASC())
    )


def event_gap_per_era():
    # Returns the average gap between events in each era, in days.
    event_info = events.CALCULATE(
        day_gap=DATEDIFF(
            "days", PREV(date_time, by=date_time.ASC(), per="eras"), date_time
        )
    )
    return (
        eras.WHERE(HAS(event_info))
        .CALCULATE(era_name=name, avg_event_gap=AVG(event_info.day_gap))
        .ORDER_BY(start_year.ASC())
    )


def pct_searches_per_tod():
    # Returns the percentage of searches belonging to each time of day,
    # ordered by the time of day.
    percentage_of_searches = ROUND(
        (100.0 * COUNT(searches)) / RELSUM(COUNT(searches)), 2
    )
    return times_of_day.CALCULATE(
        tod=name, pct_searches=percentage_of_searches
    ).ORDER_BY(start_hour.ASC())


def users_most_cold_war_searches():
    # Returns the 3 users with the most searches of cold war events.
    cold_war_searches = searches.WHERE(HAS(events.WHERE(era.name == "Cold War")))
    return (
        users.WHERE(HAS(cold_war_searches))
        .CALCULATE(user_name=name, n_cold_war_searches=COUNT(cold_war_searches))
        .TOP_K(3, by=(n_cold_war_searches.DESC(), user_name.ASC()))
    )


def intra_season_searches():
    # For each season, what percentage of all searches made in that season are
    # of events in that season, and what percentage of all searches for events
    # in that season are made in that season.
    search_info = searches.CALCULATE(
        is_intra_season=HAS(events.WHERE(season.name == season_name))
    )
    event_search_info = events.searches.CALCULATE(
        is_intra_season=season.name == season_name
    )
    return (
        seasons.CALCULATE(season_name=name)
        .CALCULATE(
            season_name,
            pct_season_searches=ROUND(
                (100.0 * SUM(search_info.is_intra_season)) / COUNT(search_info),
                2,
            ),
            pct_event_searches=ROUND(
                (100.0 * SUM(event_search_info.is_intra_season))
                / COUNT(event_search_info),
                2,
            ),
        )
        .ORDER_BY(season_name.ASC())
    )


def most_popular_topic_per_region():
    # For each user region, identifies which event type was most frequently
    # searched about by users from that region, and how many searches were
    # made for events of that topic. Break ties alphabetically by event type.
    return (
        events.CALCULATE(event_type)
        .searches.CALCULATE(region=user.region)
        .PARTITION(name="region_event_types", by=(region, event_type))
        .CALCULATE(region, event_type, n_searches=NDISTINCT(searches.search_id))
        .PARTITION(name="regions", by=region)
        .region_event_types.BEST(by=n_searches.DESC(), per="regions")
    )


def most_popular_search_engine_per_tod():
    # For each time of day, what search engine is used most often?
    # Break ties alphabetically by search engine.
    return (
        times_of_day.CALCULATE(tod=name)
        .searches.PARTITION(name="tod_search_engines", by=(tod, search_engine))
        .CALCULATE(tod, search_engine, n_searches=COUNT(searches))
        .PARTITION(name="tod", by=tod)
        .tod_search_engines.BEST(by=(n_searches.DESC(), search_engine.ASC()), per="tod")
        .ORDER_BY(tod.ASC())
    )


def unique_users_per_engine():
    # For each search engine, how many unique users used it the 2010s?
    return (
        searches.PARTITION(name="search_engines", by=search_engine)
        .CALCULATE(
            engine=search_engine,
            n_users=NDISTINCT(
                searches.WHERE(MONOTONIC(2010, YEAR(ts), 2019)).user.user_id
            ),
        )
        .ORDER_BY(engine.ASC())
    )


def overlapping_event_search_other_users_per_user():
    # For each user, count how many different users searched for the same event
    # as them. Find the 7 users with the most overlap, breaking ties
    # alphabetically.
    search_overlap_customers = searches.events.searches.user.WHERE(
        name != original_user_name
    )
    return (
        users.CALCULATE(original_user_name=name)
        .WHERE(HAS(search_overlap_customers))
        .CALCULATE(
            user_name=original_user_name,
            n_other_users=NDISTINCT(search_overlap_customers.user_id),
        )
        .TOP_K(7, by=(n_other_users.DESC(), original_user_name.ASC()))
    )


def overlapping_event_searches_per_user():
    # For each user, count how many of their searches were for an event that
    # was searched for by at least one other user. Find the 4 users with the
    # most such searches, breaking ties alphabetically.
    same_event_other_user = events.searches.user.WHERE(name != original_user_name)
    selected_searches = searches.WHERE(
        (COUNT(same_event_other_user) != 0) & HAS(same_event_other_user)
    )
    return (
        users.CALCULATE(original_user_name=name)
        .WHERE(HAS(selected_searches))
        .CALCULATE(user_name=original_user_name, n_searches=COUNT(selected_searches))
        .TOP_K(4, by=(n_searches.DESC(), original_user_name.ASC()))
    )


def search_results_by_tod():
    # For each time of day, count the percentage of all searches made during
    # that time of day and the average number of search results returned.
    return times_of_day.CALCULATE(
        tod=name,
        pct_searches=ROUND((100.0 * COUNT(searches)) / RELSUM(COUNT(searches)), 2),
        avg_results=ROUND(AVG(searches.n_results), 2),
    ).ORDER_BY(start_hour.ASC())
