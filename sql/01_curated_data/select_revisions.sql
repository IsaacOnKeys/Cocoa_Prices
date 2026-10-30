-- Input gates require one distinct business revision per feed/date/kind.
-- Arrival order and timestamp ties never select a business value.
CREATE TEMP TABLE selected_sources AS
SELECT COALESCE(s.feed,b.feed) AS feed,COALESCE(s.date,b.date) AS date,
  COALESCE(s.price,b.price) AS price,
  COALESCE(s.precipitation,b.precipitation) AS precipitation,
  COALESCE(s.soil_moisture,b.soil_moisture) AS soil_moisture
FROM (SELECT * FROM source_input WHERE kind='stage') s
FULL JOIN (SELECT * FROM source_input WHERE kind='baseline') b USING(feed,date);

CREATE TEMP TABLE candidate_cocoa AS
SELECT date AS Date,price AS Euro_Price FROM selected_sources WHERE feed='cocoa';
CREATE TEMP TABLE candidate_oil AS
SELECT date,price AS brent_price_eu FROM selected_sources WHERE feed='oil';
CREATE TEMP TABLE candidate_weather AS
SELECT date,precipitation,soil_moisture FROM selected_sources WHERE feed='weather';
