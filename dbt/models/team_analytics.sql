select
  t.team_long_name as team_name,
  count(m.id) as matches_played,
  sum(case when m.home_team_goal > m.away_team_goal then 1 else 0 end) as wins,
  sum(case when m.home_team_goal = m.away_team_goal then 1 else 0 end) as draws,
  sum(m.home_team_goal) as total_goals_scored,
  sum(m.away_team_goal) as total_goals_conceded
from
  matches m
join
  teams t on m.home_team_api_id = t.team_api_id
group by
  t.team_long_name
having
  count(m.id) > 0  -- Only include teams with matches
order by
  wins desc