-- Creates the warehouse table used by the EV charging infrastructure pipeline.
-- The Python loader can also create this table automatically with pandas.to_sql,
-- but keeping the DDL here makes the expected schema explicit.

create table if not exists public.ev_stations (
    station_id bigint,
    station_name text,
    city text,
    state text,
    latitude double precision,
    longitude double precision,
    network text,
    level2_ports integer,
    dc_fast_ports integer,
    access text,
    status text,
    station_count integer,
    infrastructure_score integer,
    fast_charging_density double precision
);

create index if not exists idx_ev_stations_state_city
    on public.ev_stations (state, city);

create index if not exists idx_ev_stations_station_id
    on public.ev_stations (station_id);

create index if not exists idx_ev_stations_infrastructure_score
    on public.ev_stations (infrastructure_score desc);
