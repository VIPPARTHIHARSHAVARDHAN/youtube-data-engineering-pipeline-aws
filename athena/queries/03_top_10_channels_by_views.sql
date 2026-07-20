SELECT
    channel_title,
    SUM(views) AS total_views
FROM final_analytics
GROUP BY channel_title
ORDER BY total_views DESC
LIMIT 10;
