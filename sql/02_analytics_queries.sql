-- Example analytics queries for the processed EV charging station table.

-- Query 1: Cities with at least 6 stations, ordered by lowest DC fast-port density.
select
    city,
    state,
    count(*) as station_name_count,
    sum(coalesce(dc_fast_ports, 0)) as dc_fast_ports_count,
    sum(coalesce(dc_fast_ports, 0))::numeric / nullif(count(*), 0) as dc_charging_port_density
from public.ev_stations
group by city, state
having count(station_name) >= 6
order by dc_charging_port_density asc, city desc;

-- Query 2: City-level estimated charging capacity score.
select
    city,
    state,
    sum(coalesce(level2_ports, 0) * 1 + coalesce(dc_fast_ports, 0) * 20) as estimated_power_score,
    count(*) as station_count,
    sum(coalesce(dc_fast_ports, 0)) as dc_port_count,
    sum(coalesce(level2_ports, 0)) as level2_port_count
from public.ev_stations
group by city, state
order by estimated_power_score desc;

-- Query 3: City-level DC fast-port totals.
select
    city,
    state,
    sum(coalesce(dc_fast_ports, 0)) as dc_fast_port_count
from public.ev_stations
group by city, state
order by dc_fast_port_count desc;

-- Query 4: Station-level estimated charging capacity score.
select
    station_name,
    city,
    state,
    coalesce(level2_ports, 0) * 1 + coalesce(dc_fast_ports, 0) * 20 as estimated_power_score
from public.ev_stations
group by station_name, city, state, level2_ports, dc_fast_ports
order by estimated_power_score desc;
