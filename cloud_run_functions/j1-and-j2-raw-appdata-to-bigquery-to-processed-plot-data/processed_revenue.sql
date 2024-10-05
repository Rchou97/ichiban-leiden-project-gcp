SELECT
  hist.Tijd,
  hist.pin,
  hist.cash,
  hist.ideal,
  hist.thuis,
  hist.total_am,
  hist.n_pin,
  hist.n_cash,
  hist.n_ideal,
  hist.n_thuis,
  hist.n_total,
  hist.avg_pin,
  hist.avg_cash,
  hist.avg_ideal,
  hist.avg_thuis,
  hist.avg_total,
  hist.`%N_pin`,
  hist.`%N_cash`,
  hist.`%N_ideal`,
  hist.`%N_thuis`,
  hist.`%am_pin`,
  hist.`%am_cash`,
  hist.`%am_ideal`,
  hist.`%am_thuis`,
  hist.temp,
  hist.`sun%`,
  hist.`rain%`,
  hist.windbft,
  hist.Comm
FROM `ichiban_revenue` AS hist
WHERE Tijd < DATE "2024-06-03"
    AND total_am IS NOT NULL

UNION ALL

SELECT
  app.Tijd,
  app.pin,
  app.cash,
  app.ideal,
  app.thuis,
  app.total_am,
  app.n_pin,
  app.n_cash,
  app.n_ideal,
  app.n_thuis,
  app.n_total,
  app.avg_pin,
  app.avg_cash,
  app.avg_ideal,
  app.avg_thuis,
  app.avg_total,
  app.`%N_pin`,
  app.`%N_cash`,
  app.`%N_ideal`,
  app.`%N_thuis`,
  app.`%am_pin`,
  app.`%am_cash`,
  app.`%am_ideal`,
  app.`%am_thuis`,
  weather.temp,
  weather.`sun%`,
  weather.`rain%`,
  weather.windbft,
  NULL AS Comm
FROM `ichiban_revenue_appdata` AS app
LEFT JOIN `weather_leiden` AS weather
    ON app.Tijd = weather.date
-- Due to legacy form
WHERE app.Tijd >= DATE "2022-06-03"
    AND total_am IS NOT NULL
ORDER BY Tijd DESC;