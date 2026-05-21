-- Basic data quality checks for the EV charging station warehouse table.
-- These are useful after the Airflow DAG finishes loading data into Supabase.

-- 1. Total loaded rows.
select
    count(*) as total_rows
from public.ev_stations;

-- 2. Duplicate station IDs.
select
    station_id,
    count(*) as duplicate_count
from public.ev_stations
where station_id is not null
group by station_id
having count(*) > 1
order by duplicate_count desc, station_id;

-- 3. Missing location fields.
select
    count(*) filter (where city is null) as missing_city,
    count(*) filter (where state is null) as missing_state,
    count(*) filter (where latitude is null) as missing_latitude,
    count(*) filter (where longitude is null) as missing_longitude
from public.ev_stations;

-- 4. Negative charger counts should not exist.
select
    station_id,
    station_name,
    city,
    state,
    level2_ports,
    dc_fast_ports
from public.ev_stations
where coalesce(level2_ports, 0) < 0
   or coalesce(dc_fast_ports, 0) < 0;

-- 5. Rows with no charger ports recorded.
select
    count(*) as stations_with_zero_recorded_ports
from public.ev_stations
where coalesce(level2_ports, 0) = 0
  and coalesce(dc_fast_ports, 0) = 0;
