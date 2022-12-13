
{{ config(
    indexes = [{'columns':['_airbyte_unique_key'],'unique':True}],
    unique_key = "_airbyte_unique_key",
    schema = "public",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('botify_visits_report') }}
with ranked_table as (
    select
        *,
        RANK() OVER(
            PARTITION BY date, url_hash, sessionmedium, sessionsource, devicecategory
            ORDER BY is_golden DESC, _airbyte_emitted_at ASC
        ) as golden_rank
from {{ ref('botify_visits_report') }}
-- botify_visits_report from {{ source('public', '_airbyte_raw_botify_visits_report') }}
where 1 = 1
)

select * 
from ranked_table
where golden_rank = 1

{{ incremental_clause('_airbyte_emitted_at', this) }}