select
    nhl_player_id as id,
    full_name,
    game_team_name as team_name,
    SUM(stats_assists) as assists,
    SUM(stats_goals) as goals,
    SUM(stats_goals + stats_assists) as points,
    SUM(concat('P0000-00-00T00:', lpad(stats_time_on_ice, 5, '0'))::INTERVAL) as time_on_ice
from {{ ref('player_game_stats') }}
where stats_goals + stats_assists > 0
and stats_time_on_ice is not Null
group by id, full_name, team_name
