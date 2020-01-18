WITH last_n_days_data AS
  (SELECT row_computed_at,
          dt,
          concat(user_id,session_id,assigned_at) as unique,user_id,assigned_at,rated_at,clubbing_flag
   FROM {dst_db_table_name}
   WHERE dt BETWEEN '{start_dt}' AND '{end_dt}'),
     aggregated_data AS
  (SELECT dt,
          COUNT(*), AS total_count,
          COUNT(DISTINCT unique) AS distinct_chat_count,
          max(assigned_at) as chat_assigned_at,
          max(rated_at) as chat_rated_at,
          count(clubbing_flag) as orders_count
   FROM last_n_days_data
   GROUP BY dt
   ORDER BY dt)
SELECT dt,
       CASE
           WHEN total_count <= (distinct_chat_count + 500) THEN 0
           ELSE 1
       END AS duplicates_flag,
       CASE
           WHEN date_add('second',86388,date_parse(dt, '%Y%m%d')) between  chat_assigned_at and date_add('second',3600,chat_assigned_at)  THEN 0 
           ELSE 1
        END AS jumbo_derived_chat_history_issue_flag,
        CASE
           WHEN date_add('second',86388,date_parse(dt, '%Y%m%d')) between rated_at and date_add('second',7200,rated_at) THEN 0 
           ELSE 1
        END AS jumbo_derived_chat_bot_events_issue_flag,
        CASE
           WHEN (count(*)-count(clubbing_flag))*(100.00)/count(*)<40.00 THEN 0 
           ELSE 1
        END AS jumbo_derived_zomato_order_history_issue_flag,
        
FROM aggregated_data
ORDER BY dt