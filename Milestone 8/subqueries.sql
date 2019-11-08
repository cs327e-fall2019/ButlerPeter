-- Query 1 (data studio)
-- returns all active QBs that have a passer rating that is above average ordered by greatest to least passer rating 
select p.player_id, p.passer_rating
from `nfl_stats_modeled.Passing_Career_Stats_Beam_DF` as p join `nfl_stats_modeled.Basic_Stats_Beam_DF` as b on p.player_id = b.player_id
where p.passer_rating > (select avg(passer_rating) from nfl_stats_modeled.Passing_Career_Stats_Beam_DF) and b.current_status = "Active" and b.position = "QB"
order by p.passer_rating desc

-- Query 2 (data studio)
-- returns all defensive players who had more than average solo tackles when they were 25 or younger
select d.player_id, d.solo_tackles, b.position
from `nfl_stats_modeled.Defensive_Career_Stats_Beam_DF` d join `nfl_stats_modeled.Basic_Stats_Beam_DF` b on d.player_id = b.player_id
where d.solo_tackles > (select avg(solo_tackles) from `nfl_stats_modeled.Defensive_Career_Stats_Beam_DF`) and b.age <= 25
order by d.solo_tackles desc

-- Query 3
-- returns total rushing yards of players who had less than average rushing attempts ordered by greatest to least rushing yards
select player_id, rushing_yards
from `nfl_stats_modeled.Rushing_Career_Stats_Beam_DF` 
where rushing_attempts < (select avg(rushing_attempts) from `nfl_stats_modeled.Rushing_Career_Stats_Beam_DF`)
order by rushing_yards desc

-- Query 4
-- returns receptions, yards per reception, and total receiving yards of players who had more than the average total receptions in a season
select player_id, receptions, yards_per_reception, receiving_yards
from `nfl_stats_modeled.Receiving_Career_Stats_Beam_DF` 
where receptions > (select avg(receptions) from `nfl_stats_modeled.Receiving_Career_Stats_Beam_DF`)
order by receiving_yards desc