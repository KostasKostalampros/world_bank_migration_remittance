/*Top 10 country_to_country by number of migrants*/

select dwh_c1.country as country_origin, dwh_c2.country as country_destination, migration
from (select country_origin_id, country_destination_id, round(migration,2) as migration
from dwh.corridor
where migration is not null
group by country_origin_id, country_destination_id, migration 
order by migration desc limit 10) as migration_figures 
join dwh.country as dwh_c1 on migration_figures.country_origin_id = dwh_c1.id
join dwh.country as dwh_c2 on migration_figures.country_destination_id = dwh_c2.id
order by migration desc;


/*Top 10 country_to_country by number of remittances*/

select dwh_c1.country as country_origin, dwh_c2.country as country_destination, remittance
from (select country_origin_id, country_destination_id, round(remittance,2) as remittance
from dwh.corridor
where remittance is not null
group by country_origin_id, country_destination_id, remittance 
order by remittance desc limit 10) as remittance_figures 
join dwh.country as dwh_c1 on remittance_figures.country_origin_id = dwh_c1.id
join dwh.country as dwh_c2 on remittance_figures.country_destination_id = dwh_c2.id
order by remittance desc;


/*Top 10 sending countries in terms of migrants*/

select dwh_c1.country as country_origin, migration_figures.total_migration as total_migration
from (select country_origin_id, round(sum(migration),2) total_migration
from dwh.corridor
where migration is not null
group by country_origin_id
order by total_migration DESC
limit 10) as migration_figures 
join dwh.country as dwh_c1 on migration_figures.country_origin_id = dwh_c1.id
order by total_migration desc;


/*Top 10 sending countries in terms of remittance*/

select dwh_c1.country as country_origin, remittance_figures.total_remittance as total_remittance
from (select country_origin_id, round(sum(remittance),2) total_remittance
from dwh.corridor
where remittance is not null
group by country_origin_id
order by total_remittance DESC
limit 10) as remittance_figures 
join dwh.country as dwh_c1 on remittance_figures.country_origin_id = dwh_c1.id
order by total_remittance desc;


/*Top 10 receiving countries in terms of migrants*/

select dwh_c1.country as country_destination, migration_figures.total_migration as total_migration
from (select country_destination_id, round(sum(migration),2) total_migration
from dwh.corridor
where migration is not null
group by country_destination_id
order by total_migration DESC
limit 10) as migration_figures 
join dwh.country as dwh_c1 on migration_figures.country_destination_id = dwh_c1.id
order by total_migration desc;


/*Top 10 receiving countries in terms of remittance*/

select dwh_c1.country as country_destination, remittance_figures.total_remittance as total_remittance
from (select country_destination_id, round(sum(remittance),2) total_remittance
from dwh.corridor
where remittance is not null
group by country_destination_id
order by total_remittance DESC
limit 10) as remittance_figures 
join dwh.country as dwh_c1 on remittance_figures.country_destination_id = dwh_c1.id
order by total_remittance desc;


/* Top 10 Net Senders in terms of migrants*/

select dwh_c1.country as country_origin, migration_figures.net_migration as net_migration
from (select t1.country_origin_id as country_origin_id, round((t1.total_migration - t2.total_migration),2) as net_migration
from (select country_origin_id, sum(migration) total_migration
from dwh.corridor
group by country_origin_id
order by total_migration DESC) as t1
join 
(select country_destination_id, sum(migration) total_migration
from dwh.corridor
where migration is not null
group by country_destination_id
order by total_migration DESC) as t2
on t1.country_origin_id = t2.country_destination_id
order by net_migration desc
limit 10) as migration_figures 
join dwh.country as dwh_c1 on migration_figures.country_origin_id = dwh_c1.id
order by net_migration desc;


/* Top 10 Net Senders in terms of remittance*/

select dwh_c1.country as country_origin, remittance_figures.net_remittance as net_remittance
from (select t1.country_origin_id, round((t1.total_remittance - t2.total_remittance),2) as net_remittance
from (select country_origin_id, sum(remittance) total_remittance
from dwh.corridor
where remittance is not null
group by country_origin_id
order by total_remittance DESC) as t1
join 
(select country_destination_id, sum(remittance) total_remittance
from dwh.corridor
where remittance is not null
group by country_destination_id
order by total_remittance DESC) as t2
on t1.country_origin_id = t2.country_destination_id
order by net_remittance desc
limit 10) as remittance_figures 
join dwh.country as dwh_c1 on remittance_figures.country_origin_id = dwh_c1.id
order by net_remittance desc;


/* Top 10 Net Receivers in terms of migrants*/

select dwh_c1.country as country_origin, migration_figures.net_migration as net_migration
from (select t1.country_origin_id as country_origin_id, round((t2.total_migration - t1.total_migration),2) as net_migration
from (select country_origin_id, sum(migration) total_migration
from dwh.corridor
group by country_origin_id
order by total_migration DESC) as t1
join 
(select country_destination_id, sum(migration) total_migration
from dwh.corridor
where migration is not null
group by country_destination_id
order by total_migration DESC) as t2
on t1.country_origin_id = t2.country_destination_id
order by net_migration desc
limit 10) as migration_figures 
join dwh.country as dwh_c1 on migration_figures.country_origin_id = dwh_c1.id
order by net_migration desc;


/* Top 10 Net Receivers in terms of remittance*/

select dwh_c1.country as country_origin, remittance_figures.net_remittance as net_remittance
from (select t1.country_origin_id, round((t2.total_remittance - t1.total_remittance),2) as net_remittance
from (select country_origin_id, sum(remittance) total_remittance
from dwh.corridor
where remittance is not null
group by country_origin_id
order by total_remittance DESC) as t1
join 
(select country_destination_id, sum(remittance) total_remittance
from dwh.corridor
where remittance is not null
group by country_destination_id
order by total_remittance DESC) as t2
on t1.country_origin_id = t2.country_destination_id
order by net_remittance DESC
limit 10) as remittance_figures 
join dwh.country as dwh_c1 on remittance_figures.country_origin_id = dwh_c1.id
order by net_remittance desc;