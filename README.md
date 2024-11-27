# Livestream Watch Time Analysis

## Project Overview

This project aims to analyze livestream watch times of users across different regions. The goal is to identify the top 5 users with the highest watch time for each region. The analysis involves multiple SQL transformations to extract meaningful insights from the data.

## Steps in the Query

### 1. Livestream Watch Time Calculation
In this step, we calculate the time each user spent in a livestream session.  
- Combine the `livestream_enter` and `livestream_exit` tables using a `LEFT JOIN`.  
- For users who are still in the livestream, use the current timestamp as their `exit_time`.  
- Ensure that `exit_time` is always greater than or equal to `enter_time`.  
- Group by the `user_id`, `livestream_id`, and `enter_time` to compute session-specific watch times.

```sql
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
)
```

### 2. Region Mapping
Here, we map users to their respective regions based on their app entry times.  

- Use the `user_entry` table to determine the region for each user session.  
- Ensure that the `livestream_enter` time is greater than or equal to the `user_entry` time.  
- Use the `LEAD` function to calculate the next entry time, which helps identify the active region during overlapping sessions.  

```sql
UserRegion AS (
    SELECT
        ue.user_id,
        ue.event_time AS entry_time,
        ue.region,
        LEAD(ue.event_time) OVER (PARTITION BY ue.user_id ORDER BY ue.event_time) AS next_entry_time
    FROM xxx.user_entry ue
)
```

### 3. Combine Watch Time with Region
In this step, we combine the watch time data with region information.  

- Join the `LivestreamWatchTime` and `UserRegion` tables to associate each livestream session with the correct region.  
- Ensure that the `livestream_enter` time falls between the `entry_time` and the `next_entry_time` for the region.  

```sql
WatchTimeWithRegion AS (
    SELECT
        lwt.user_id,
        ur.region,
        TIMESTAMP_DIFF(lwt.exit_time, lwt.enter_time, SECOND) / 60 AS watch_time_minutes
    FROM LivestreamWatchTime lwt
    LEFT JOIN UserRegion ur ON lwt.user_id = ur.user_id
        AND lwt.enter_time >= ur.entry_time
        AND (lwt.enter_time < ur.next_entry_time OR ur.next_entry_time IS NULL)
)
```

### 4. Total Watch Time by Region and User
Next, we calculate the total watch time for each user in each region.  

- Group the data by `region` and `user_id`.  
- Sum up the watch time in minutes for all sessions.  

```sql
TotalWatchTime AS (
    SELECT
        region,
        user_id,
        SUM(watch_time_minutes) AS total_watch_time_minutes
    FROM WatchTimeWithRegion
    GROUP BY region, user_id
)
```

### 6. Retrieve Top 5 Users per Region
Finally, we extract the top 5 users with the highest watch times in each region.  

- Filter the ranked data to include only users with ranks 1 through 5.  
- Sort the results by `region` and descending `total_watch_time_minutes`.  

```sql
SELECT
    region AS Region,
    user_id AS UserID,
    total_watch_time_minutes AS TotalWatchTimeMinutes
FROM RankedWatchTime
WHERE rank <= 5
ORDER BY region ASC, total_watch_time_minutes DESC;
```

## Full Query
The full query integrates all the steps outlined above, with the aim to retrieve the top 5 users by total watch time in each region. You can find the full SQL code in the main under `final-query`.

The logic involves the following key steps:
1. **LivestreamWatchTime:** This table captures each user's entry and exit time for livestreams.
2. **UserRegion:** Maps users to their regions based on entry times, and calculates the next region entry to handle overlapping sessions.
3. **WatchTimeWithRegion:** Combines the watch time data with regions to calculate total minutes watched by each user in each region.
4. **TotalWatchTime:** Aggregates the total watch time for each user per region.
5. **RankedWatchTime:** Ranks users within each region based on their total watch time in descending order.
6. **Final Output:** Filters the top 5 ranked users per region and outputs their details.

### How to Use the Query
To use the full query:
1. Download or clone the project repository.
2. Navigate to the `queries/` directory.
3. Open the query file for review or execution in your SQL environment.
4. Make sure to replace the placeholder for your company's database (e.g., `xxx` in the query) with the appropriate schema name.
5. Run the query in your SQL engine to generate the results.

### Expected Output
The query produces a table with the following columns:
- **Region:** The region where the user accessed the livestream.
- **UserID:** The unique identifier for the user.
- **TotalWatchTimeMinutes:** The total watch time (in minutes) for the user in the region.

You can then use the results to identify the top users per region, which is useful for targeting specific regions or rewarding high-engagement users.

## Use Case
This query is designed for analysis in live streaming platforms or applications that track user interactions across different regions. It helps:
- Identify the most engaged users in each region.
- Tailor content, advertisements, or rewards to specific regions with high user engagement.
- Optimize livestream strategies by focusing on regions or users that contribute the most watch time.

## Additional Notes
- Ensure that your database tables (`livestream_enter`, `livestream_exit`, `user_entry`) are properly indexed to improve query performance.
- Depending on your data size, consider optimizing the query using partitioning or caching for faster results.

## Conclusion
This project provides valuable insights into user behavior during livestream events, specifically by identifying the top users in each region based on their total watch time. The query can be customized further to meet your specific needs, such as adjusting the number of top users or including additional user metadata.
