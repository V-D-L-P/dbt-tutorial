{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "public",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('botify_visits_report_ab3') }}
select
    url,
    {{ adapter.quote('date') }},
    host,
    {{ adapter.quote('path') }},
    uuid,
    device,
    medium,
    {{ adapter.quote('source') }},
    sessions,
    url_hash,
    is_golden,
    new_users,
    page_views,
    data_source,
    sync_timestamp,
    engagedsessions,
    session_duration,
    goal_completions_all,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_botify_visits_report_hashid
from {{ ref('botify_visits_report_ab3') }}
-- botify_visits_report from {{ source('public', '_airbyte_raw_botify_visits_report') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

