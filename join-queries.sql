-- returns name and passer rating of NFL players with a career passer rating greater than 100 that attend college at Texas
select b.name, p.passer_rating
from nfl_stats_modeled.Basic_Stats as b
join nfl_stats_modeled.Passing_Career_Stats as p on b.player_id = p.player_id
where p.passer_rating > 100 and b.college = "Texas"
order by p.passer_rating desc

-- returns name, height, and rushing attempts per game of runningbacks under 6 feet tall with more than 15 rushing attempts per game
select b.name, b.height__inches, r.rushing_attempts_per_game
from nfl_stats_modeled.Basic_Stats as b
join nfl_stats_modeled.Rushing_Career_Stats as r on b.player_id = r.player_id
where r.rushing_attempts_per_game > 15 and b.height__inches < 72
order by b.height__inches

-- returns name, weight, and passer rating of Quarterbacks that weigh 220+ lbs and have a passer rating greater than 100
select b.name, b.weight__lbs, p.passer_rating
from nfl_stats_modeled.Basic_Stats as b
join nfl_stats_modeled.Passing_Career_Stats as p on b.player_id = p.player_id
where p.passer_rating > 100 and b.weight__lbs >= 220
order by p.passer_rating desc

-- returns name, age, and games played of quarterbacks age 35 and up including null values under games played
select b.name, b.age, c.games_played
from nfl_stats_modeled.Basic_Stats as b
full join nfl_stats_modeled.Passing_Career_Stats as c on b.player_id = c.player_id
where b.age >= 35
order by age desc

-- returns name, weight, rushing attempts per game of runningbacks that weigh 220+ lbs from greatest to least rushing attempts per game including null values under rushing attempts per game
select b.name, b.weight__lbs, c.rushing_attempts_per_game
from nfl_stats_modeled.Basic_Stats as b
full join nfl_stats_modeled.Rushing_Career_Stats as c on b.player_id = c.player_id
where b.weight__lbs >= 220
order by c.rushing_attempts_per_game desc

-- returns name and receiving yards per game of quarterbacks ordered from greatest to least receiving yards per game
select b.name, c.yards_per_game
from nfl_stats_modeled.Basic_Stats as b
full join nfl_stats_modeled.Receiving_Career_Stats as c on b.player_id = c.player_id
where b.position = "QB"
order by c.yards_per_game desc