-- Query 1 (data studio)
-- finds the average weight of each active player by position 
select position, avg(weight__lbs) as avg_weight
from nfl_stats_modeled.Basic_Stats_Beam
where current_status = 'Active'
group by position
order by avg_weight desc

-- Query 2
-- returns count of all NFL players who played all 16 games in a season
select count(player_id)
from nfl_stats_modeled.Season_Stats_Beam_DF
where games_played = 16

-- Query 3
-- returns average yards per reception of all QBs, TEs, WRs grouped by position
select position, avg(yards_per_reception) as avg_yards_per_reception
from `nfl_stats_modeled.Rushing_Career_Stats_Beam_DF` a join `nfl_stats_modeled.Receiving_Career_Stats_Beam_DF` b on a.player_id = b.player_id
group by position
having position in("RB", "WR", "TE")

-- Query 4 
-- returns max rushing yards of all runningbacks since 2000 who played 10 games or less
select max(r.rushing_yards)
from `nfl_stats_modeled.Rushing_Career_Stats_Beam_DF` r join `nfl_stats_modeled.Season_Stats_Beam_DF` s on r.player_id = s.player_id
where s.games_played <= 10
group by r.year
having r.year >= 2000

-- Query 5 (data studio)
-- finds the total rushing yards of the top 25 players having more than 0 rushing yards in their career 
select a.name, sum(b.rushing_yards) as Career_Rushing_Yards
from  nfl_stats_modeled.Basic_Stats_Beam_DF a join nfl_stats_modeled.Rushing_Career_Stats_Beam_DF b
on a.player_id = b.player_id
group by a.name
having Career_Rushing_Yards > 0
order by Career_Rushing_Yards desc
limit 25

-- Query 6
-- returns total career touchdowns from the top 25 Quarterbacks of all time (data studio)
select b.name, sum(p.td_passes) as total_tds
from  `nfl_stats_modeled.Basic_Stats_Beam_DF` b join `nfl_stats_modeled.Passing_Career_Stats_Beam_DF` p on b.player_id = p.player_id
where b.position = "QB"
group by b.name
having total_tds > 0
order by total_tds desc
limit 25



