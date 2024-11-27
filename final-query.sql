WITH LivestreamWatchTime AS (
    SELECT
        le.user_id,
        le.livestream_id,
        le.event_time AS enter_time,
        COALESCE(MIN(le_exit.event_time), CURRENT_TIMESTAMP()) AS exit_time
    FROM xxx.livestream_enter le
    LEFT JOIN xxx.livestream_exit le_exit ON le.user_id = le_exit.user_id
        AND le.livestream_id = le_exit.livestream_id
        AND le_exit.event_time >= le.event_time
    GROUP BY le.user_id, le.livestream_id, le.event_time
),
UserRegion AS (
    SELECT
        ue.user_id,
        ue.event_time AS entry_time,
        ue.region,
        LEAD(ue.event_time) OVER (PARTITION BY ue.user_id ORDER BY ue.event_time) AS next_entry_time
    FROM xxx.user_entry ue
),
WatchTimeWithRegion AS (
    SELECT
        lwt.user_id,
        ur.region,
        TIMESTAMP_DIFF(lwt.exit_time, lwt.enter_time, SECOND) / 60 AS watch_time_minutes
    FROM LivestreamWatchTime lwt
    LEFT JOIN UserRegion ur ON lwt.user_id = ur.user_id
        AND lwt.enter_time >= ur.entry_time
        AND (lwt.enter_time < ur.next_entry_time OR ur.next_entry_time IS NULL)
),
TotalWatchTime AS (
    SELECT
        region,
        user_id,
        SUM(watch_time_minutes) AS total_watch_time_minutes
    FROM WatchTimeWithRegion
    GROUP BY region, user_id
),
RankedWatchTime AS (
    SELECT
        region,
        user_id,
        total_watch_time_minutes,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_watch_time_minutes DESC) AS rank
    FROM TotalWatchTime
)
SELECT
    region AS Region,
    user_id AS UserID,
    total_watch_time_minutes AS TotalWatchTimeMinutes
FROM RankedWatchTime
WHERE rank <= 5
ORDER BY region ASC, total_watch_time_minutes DESC;
