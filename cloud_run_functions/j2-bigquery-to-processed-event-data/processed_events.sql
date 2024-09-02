SELECT
  sh.name_holiday AS name_event,
  sh.start_date AS start_date,
  sh.end_date AS end_date,
  sh.holiday_type AS event_type,
  sh.source AS `source`,
  NULL AS url
FROM `school_holidays` AS sh

UNION ALL

SELECT
  ph.name_holiday AS name_event,
  ph.start_date AS start_date,
  ph.end_date AS end_date,
  ph.holiday_type AS event_type,
  ph.source AS `source`,
  NULL AS url
FROM `public_holidays` AS ph

UNION ALL

SELECT
  le.name_event AS name_event,
  le.start_date AS start_date,
  le.end_date AS end_date,
  le.event_type AS event_type,
  le.source AS `source`,
  le.url AS url
FROM `leiden_events` AS le;