CREATE SCHEMA dwh;

CREATE TABLE dwh.country (LIKE staging.country INCLUDING ALL);

INSERT INTO dwh.country
SELECT * FROM staging.country;

CREATE table dwh.corridor as 
	SELECT COALESCE(t1.date, t2.date) as date, 
    COALESCE(t1.country_origin_id, t2.country_origin_id) as country_origin_id,
	COALESCE(t1.country_destination_id, t2.country_destination_id) as country_destination_id,
	t1.migration as migration, 
	t2.remittance as remittance
	FROM staging.migration as t1 FULL OUTER JOIN staging.remittance as t2
	ON t1.date = t2.date AND 
	t1.country_origin_id = t2.country_origin_id AND
	t1.country_destination_id = t2.country_destination_id;