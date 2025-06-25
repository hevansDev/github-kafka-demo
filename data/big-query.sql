WITH daily_events AS (
  -- Get daily counts for both stars and PRs
  SELECT 
    DATE(created_at) as event_date,
    COUNT(CASE WHEN type = 'WatchEvent' THEN 1 END) as stars_gained_that_day,
    COUNT(CASE 
      WHEN type = 'PullRequestEvent' 
      AND JSON_EXTRACT_SCALAR(payload, '$.action') = 'opened' 
      THEN 1 
    END) as prs_opened_that_day
  FROM `githubarchive.year.*` 
  WHERE 
    repo.name = 'apache/kafka'
    AND (
      type = 'WatchEvent' 
      OR (type = 'PullRequestEvent' AND JSON_EXTRACT_SCALAR(payload, '$.action') = 'opened')
    )
    AND _TABLE_SUFFIX BETWEEN '2011' AND FORMAT_DATE('%Y', CURRENT_DATE())
  GROUP BY DATE(created_at)
),
all_dates AS (
  -- Generate complete date range to fill gaps
  SELECT 
    date_val as event_date
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      (SELECT MIN(event_date) FROM daily_events),
      (SELECT MAX(event_date) FROM daily_events),
      INTERVAL 1 DAY
    )
  ) AS date_val
)
SELECT 
  'apache/kafka' as repo,
  d.event_date as date,
  COALESCE(e.stars_gained_that_day, 0) as stars_gained_that_day,
  COALESCE(e.prs_opened_that_day, 0) as prs_opened_that_day,
  -- Calculate cumulative totals (running sums)
  SUM(COALESCE(e.stars_gained_that_day, 0)) OVER (
    ORDER BY d.event_date 
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) as cumulative_stars,
  SUM(COALESCE(e.prs_opened_that_day, 0)) OVER (
    ORDER BY d.event_date 
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) as cumulative_prs
FROM all_dates d
LEFT JOIN daily_events e ON d.event_date = e.event_date
ORDER BY d.event_date;