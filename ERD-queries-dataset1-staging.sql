-- returns number of rows in Basic Stats table (17172)
select count(*) from nfl_stats_staging.Basic_Stats

-- returns count of unique player ids (17172)
select count(player_id) unique from nfl_stats_staging.Basic_Stats

-- returns count of unique player ids from defensive career stats table (23998)
select distinct count(player_id) from nfl_stats_staging.Defensive_Career_Stats

-- returns count of unique player ids from defensive career stats table that have an equivalent value in basic stats table (23998)
select distinct count(d.player_id)
from nfl_stats_staging.Defensive_Career_Stats as d, nfl_stats_staging.Basic_Stats as b
where d.player_id = b.player_id

-- returns count of unique player ids from passing career stats table (8525)
select distinct count(p.player_id)
from nfl_stats_staging.Passing_Career_Stats as p

-- returns count of unique player ids from passing career stats table that have an equivalent value in basic stats table (8525)
select distinct count(p.player_id)
from nfl_stats_staging.Passing_Career_Stats as p, nfl_stats_staging.Basic_Stats as b
where p.player_id = b.player_id

-- returns count of unique player ids from quarterback game logs table (40247)
select distinct count(p.player_id)
from nfl_stats_staging.Quarterback_Game_Logs as p

-- returns count of unique player ids from quarterback game logs table that have an equivalent value in basic stats table (40247)
select distinct count(p.player_id)
from nfl_stats_staging.Quarterback_Game_Logs as p, nfl_stats_staging.Basic_Stats as b
where p.player_id = b.player_id

-- returns count of unique player ids from receiving career stats table (18128)
select distinct count(p.player_id)
from nfl_stats_staging.Receiving_Career_Stats as p

-- returns count of unique player ids from receiving career stats table that have an equivalent value in basic stats table (18128)
select distinct count(p.player_id)
from nfl_stats_staging.Receiving_Career_Stats as p, nfl_stats_staging.Basic_Stats as b
where p.player_id = b.player_id

-- returns count of unique player ids from runningback game logs table (67661)
select distinct count(player_id)
from nfl_stats_staging.Runningback_Game_Logs as p

-- returns count of unique player ids from runningback game logs table that have an equivalent value in basic stats table (67661)
select distinct count(p.player_id)
from nfl_stats_staging.Runningback_Game_Logs as p, nfl_stats_staging.Basic_Stats as b
where p.player_id = b.player_id

-- returns count of unique player ids from rushing career stats table (17507)
select distinct count(p.player_id)
from nfl_stats_staging.Rushing_Career_Stats as p

-- returns count of unique player ids from rushing career stats table that have an equivalent value in basic stats table (17507)
select distinct count(p.player_id)
from nfl_stats_staging.Rushing_Career_Stats as p, nfl_stats_staging.Basic_Stats as b
where p.player_id = b.player_id