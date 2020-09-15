select p.team_name,
       p.full_name,
       p.points
from {{ ref('nhl_players') }} p
join (select team_name,
      max(points) as max_points
      from {{ ref('nhl_players') }}
      group by team_name
    ) x
on
    x.team_name = p.team_name and
    x.max_points = p.points and
    x.max_points > 0