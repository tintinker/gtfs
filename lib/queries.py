def delay_query(stop_id = None, trip_id = None):
    return f"""
    WITH LatestTripUpdate AS (
        SELECT
            stop_time_updates.stop_id AS update_stop_id,
            stops.stop_name,
            trip_updates.trip_id,
            trip_updates.oid,
            routes.route_short_name as route_name,
            trips.route_id,
            stop_time_updates.stop_sequence AS update_stop_sequence,
            stop_time_updates.arrival_time AS actual_arrival_time,
            CAST(strftime('%d', stop_time_updates.arrival_time) AS INTEGER) AS current_day,
            CAST(strftime('%m', stop_time_updates.arrival_time) AS INTEGER) AS current_month,
            CAST(strftime('%Y', stop_time_updates.arrival_time) AS INTEGER) AS current_year,
            stop_time_updates.arrival_seconds_since_midnight - stop_times.arrival_time as delay,
            stop_times.arrival_time as planned_arrival_seconds_since_midnight,
            STRFTIME('%H:%M', stop_time_updates.arrival_time) AS update_arrival_time,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    trip_updates.trip_id, 
                    stop_time_updates.stop_id, 
                    stop_times.arrival_time,
                    CAST(strftime('%d', stop_time_updates.arrival_time) AS INTEGER),
                    CAST(strftime('%m', stop_time_updates.arrival_time) AS INTEGER),
                    CAST(strftime('%Y', stop_time_updates.arrival_time) AS INTEGER)
                ORDER BY trip_updates.oid DESC
            ) AS rn
        FROM
            stop_time_updates
        JOIN
            trip_updates ON trip_updates.oid = stop_time_updates.trip_update_id
        JOIN
            trips ON trips.trip_id = trip_updates.trip_id
        JOIN
            stops ON stops.stop_id = stop_time_updates.stop_id
        JOIN
            routes on routes.route_id = trips.route_id
        JOIN
            stop_times ON stop_times.trip_id = trips.trip_id
                    AND stop_times.stop_id = stop_time_updates.stop_id
                    AND stop_times.stop_sequence = stop_time_updates.stop_sequence
        WHERE
            stop_time_updates.arrival_time < CURRENT_TIMESTAMP
        
    )
    SELECT
        update_stop_id as stop_id,
        stop_name,
        trip_id,
        oid,
        route_name,
        route_id,
        update_stop_sequence as trip_sequence,
        round(delay / 60, 1) as minute_delay,
        planned_arrival_seconds_since_midnight,
        actual_arrival_time,
        current_day,
        current_month,
        current_year
    FROM LatestTripUpdate
    WHERE rn = 1
    ORDER BY current_year, current_month, current_day, trip_id, planned_arrival_seconds_since_midnight;
    """