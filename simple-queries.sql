-- returns NFL players that attended college at the University of Texas ordered by name

select name from nfl_stats_staging.Basic_Stats where college = "Texas" order by name

-- returns Defensive players that did not miss a game during that season ordered by name

select name from nfl_stats_staging.Defensive_Career_Stats where games_played = 16 order by name

-- returns Quarterbacks and their passer rating with passer ratings greater than 80 ordered by passer rating

select name, passer_rating from nfl_stats_staging.Passing_Career_Stats where passer_rating > 80 order by passer_rating DESC

-- returns Quarterbacks who won an away game ordered by name

select name from nfl_stats_staging.Quarterback_Game_Logs where outcome = "W" and home_or_away = "Away" order by name

-- returns Receiver names and yards per game that have greater than or equal to 100 yards per game ordered from greatest to least

select name, yards_per_game from nfl_stats_staging.Receiving_Career_Stats where yards_per_game >= 100 order by yards_per_game desc

-- returns Runningbacks who scored 4 touchdowns in a single game ordered by name

select name from nfl_stats_staging.Runningback_Game_Logs where rushing_tds = "4" order by name

-- returns name and rushing attempts per game of Runningbacks that have over 20 rushing attempts per game ordered by attempts from greatest to least

select name, rushing_attempts_per_game from nfl_stats_staging.Rushing_Career_Stats where rushing_attempts_per_game > 20 order by rushing_attempts_per_game desc