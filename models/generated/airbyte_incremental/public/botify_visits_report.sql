{{ config(
    indexes = [{'columns':['_airbyte_unique_key'],'unique':True}],
    unique_key = "_airbyte_unique_key",
    schema = "public",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('botify_visits_report_scd') }}
select
    _airbyte_unique_key,
    url,
    {{ adapter.quote('date') }},
    uuid,
    hostname,
    newusers,
    sessions,
    url_hash,
    is_golden,
    conversions,
    landingpage,
    sessionmedium,
    sessionsource,
    devicecategory,
    engagedsessions,
    screenpageviews,
    userengagementduration,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_botify_visits_report_hashid
from {{ ref('botify_visits_report_scd') }}
-- botify_visits_report from {{ source('public', '_airbyte_raw_botify_visits_report') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

