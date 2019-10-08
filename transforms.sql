-- creates new Basic Stats table with the same entries as original Basic Stats table
create table nfl_stats_modeled.Basic_Stats as
select *
from nfl_stats_staging.Basic_Stats b
order by b.player_id

-- creates a new Defensive Career Stats table with only necessary defensive stats and player_id, year as a primary key
create table nfl_stats_modeled.Defensive_Career_Stats as 
select distinct c.player_id, b.year, b.total_tackles, b.solo_tackles, b.assisted_tackles, b.sacks, b.safties, b.passes_defended, b.ints, b.ints_for_tds, b.int_yards, b.yards_per_int, b.longest_int_return
from nfl_stats_staging.Defensive_Career_Stats as b,
(select distinct player_id, year from nfl_stats_staging.Defensive_Career_Stats) as c
where b.player_id = c.player_id and b.year = c.year
order by c.player_id, b.year

-- creates new Passing Career Stats table with only passing stats and player_id, year as a primary key
create table nfl_stats_modeled.Passing_Career_Stats as 
select distinct c.player_id, b.year, b.passes_attempted , b.passes_completed, b.completion_percentage, b.pass_attempts_per_game, b.passing_yards, b.passing_yards_per_attempt, b.passing_yards_per_game , b.td_passes , b.percentage_of_tds_per_attempts , b.ints , b.int_rate, b.longest_pass, b.passes_longer_than_20_yards, b.passes_longer_than_40_yards, b.sacks, b.sacked_yards_lost, b.passer_rating
from nfl_stats_staging.Passing_Career_Stats as b,
(select distinct player_id, year from nfl_stats_staging.Passing_Career_Stats) as c
where b.player_id = c.player_id and b.year = c.year
order by c.player_id, b.year

-- creates new Quarterback Game Logs table with only relevent game stats and uses player_id, year, season, game, week as a primary key;
create table nfl_stats_modeled.Quarterback_Game_Logs as 
select distinct c.player_id, b.year, b.season , b.week, b.game_date, b.home_or_away, b.opponent, b.outcome, b.score , b.games_started , b.passes_completed , b.passes_attempted , b.completion_percentage , b.passing_yards , b.passing_yards_per_attempt , b.td_passes , b.ints , b.sacks, b.sacked_yards_lost, b.passer_rating, b.rushing_attempts, b.rushing_yards, b.yards_per_carry, b.rushing_tds, b.fumbles, b.fumbles_lost
from nfl_stats_staging.Quarterback_Game_Logs as b,
(select distinct player_id, year, season, week from nfl_stats_staging.Quarterback_Game_Logs) as c
where b.player_id = c.player_id and b.year = c.year and b.season = c.season and b.week = c.week
order by c.player_id, b.year, b.season, b.week

-- creates new Runningback Game Logs table with only relevent game stats and uses player_id, year, season, game, week as a primary key
create table nfl_stats_modeled.Runningback_Game_Logs as 
select distinct c.player_id, b.year, b.season , b.week, b.game_date, b.home_or_away, b.opponent, b.outcome, b.score , b.games_started , b.rushing_attempts , b.rushing_yards , b.yards_per_carry , b.longest_run , b.rushing_tds , b.receptions , b.receiving_yards , b.yards_per_reception , b.longest_reception , b.receiving_tds , b.fumbles , b.fumbles_lost
from nfl_stats_staging.Runningback_Game_Logs as b,
(select distinct player_id, year, season, week from nfl_stats_staging.Runningback_Game_Logs) as c
where b.player_id = c.player_id and b.year = c.year and b.season = c.season and b.week = c.week
order by c.player_id, b.year, b.season, b.week

-- creates new Receiving Career Stats table with only receiving stats and player_id, year as a primary key
create table nfl_stats_modeled.Receiving_Career_Stats as 
select distinct c.player_id, b.year , b.receptions, b.receiving_yards , b.yards_per_reception , b.yards_per_game , b.longest_reception , b.receiving_tds , b.receptions_longer_than_20_yards , b.receptions_longer_than_40_yards , b.first_down_receptions , b.fumbles
from nfl_stats_staging.Receiving_Career_Stats as b,
(select distinct player_id, year from nfl_stats_staging.Receiving_Career_Stats) as c
where b.player_id = c.player_id and b.year = c.year
order by c.player_id, b.year

-- creates new Rushing Career Stats table with only rushing stats and player_id, year as a primary key
create table nfl_stats_modeled.Rushing_Career_Stats as 
select distinct c.player_id, b.year, b.position , b.rushing_attempts , b.rushing_attempts_per_game , b.rushing_yards , b.yards_per_carry , b.rushing_yards_per_game , b.rushing_tds , b.longest_run , b.rushing_first_downs , b.percentage_of_rushing_first_downs , b.rushing_more_than_20_yards, b.rushing_more_than_40_yards, b.fumbles
from nfl_stats_staging.Rushing_Career_Stats as b,
(select distinct player_id, year from nfl_stats_staging.Rushing_Career_Stats) as c
where b.player_id = c.player_id and b.year = c.year
order by c.player_id, b.year

-- creates Season table that displays player id, year, and games_played
create table nfl_stats_modeled.Season_Stats as
select player_id, year, games_played from nfl_stats_staging.Defensive_Career_Stats
union all
select player_id, year, games_played from nfl_stats_staging.Passing_Career_Stats
union all
select player_id, year, games_played from nfl_stats_staging.Receiving_Career_Stats
union all
select player_id, year, games_played from nfl_stats_staging.Rushing_Career_Stats
union all
select player_id, year, games_played from nfl_stats_staging.Rushing_Career_Stats
union all
select player_id, year, games_played from nfl_stats_staging.Quarterback_Game_Logs
union all
select player_id, year, games_played from nfl_stats_staging.Runningback_Game_Logs