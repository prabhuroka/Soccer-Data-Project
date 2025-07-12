with player_stats as (
  select 
    p.player_name,
    count(m.id) as matches_played,
    sum(m.home_team_goal) as total_goals
    -- Removed shots since we don't have that column
  from 
    matches m
  join 
    players p on m.home_player_1 = p.id or
                 m.home_player_2 = p.id or
                 m.home_player_3 = p.id or
                 m.home_player_4 = p.id or
                 m.home_player_5 = p.id or
                 m.home_player_6 = p.id or
                 m.home_player_7 = p.id or
                 m.home_player_8 = p.id or
                 m.home_player_9 = p.id or
                 m.home_player_10 = p.id or
                 m.home_player_11 = p.id
  group by 
    p.player_name
)

select 
  player_name,
  matches_played,
  total_goals,
  (total_goals / nullif(matches_played, 0)) as goals_per_match
from 
  player_stats
where
  matches_played > 0  -- Ensure we don't divide by zero
order by 
  total_goals desc
limit 50