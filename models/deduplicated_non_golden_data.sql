
with ranked_table as (
    select
        *,
        RANK() OVER(
            PARTITION BY date
            ORDER BY sync_timestamp DESC
        ) as golden_rank
from {{ ref('botify_visits_report') }}
-- botify_visits_report from {{ source('public', '_airbyte_raw_botify_visits_report') }}
)

select * 
from ranked_table
where golden_rank = 1
