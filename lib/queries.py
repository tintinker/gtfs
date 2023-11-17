def delay_query(stop_id = None, trip_id = None):
    return f"""
    WITH LatestTripUpdate AS (
        SELECT
            stop_time_updates.stop_id AS update_stop_id,
            stop_times.stop_name,
            trip_updates.trip_id,
            trip_updates.oid,
            trips.route_name,
            trips.route_id,
            stop_time_updates.stop_sequence AS update_stop_sequence,
            stop_time_updates.arrival_seconds_since_midnight - stop_times.arrival_seconds_since_midnight as delay,
            STRFTIME('%H:%M', stop_times.arrival_timestamp) AS stop_arrival_time,
            STRFTIME('%H:%M', stop_time_updates.arrival_time) AS update_arrival_time,
            ROW_NUMBER() OVER (PARTITION BY trip_updates.trip_id, stop_time_updates.stop_id, DATE(stop_times.arrival_timestamp) ORDER BY trip_updates.oid DESC) AS rn
        FROM
            stop_time_updates
        JOIN
            trip_updates ON trip_updates.oid = stop_time_updates.trip_update_id
        JOIN
            trips ON trips.trip_id = trip_updates.trip_id
        JOIN
            stop_times ON stop_times.trip_id = trips.trip_id
                    AND stop_times.stop_id = stop_time_updates.stop_id
                    AND stop_times.stop_sequence = stop_time_updates.stop_sequence
        {'WHERE' if stop_id else ''}
            {'update_stop_id = '+str(stop_id) if stop_id else ''}
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
        stop_arrival_time as planned_arrival_time,
        update_arrival_time as actual_arrival_time
    FROM LatestTripUpdate
    WHERE rn = 1
    {'AND trip_id = "'+str(trip_id)+'"' if trip_id else ''}
    ORDER BY trip_id, stop_arrival_time;
    """