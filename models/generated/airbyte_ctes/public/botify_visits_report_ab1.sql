{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('public', '_airbyte_raw_botify_visits_report') }}
select
    {{ json_extract_scalar('_airbyte_data', ['url'], ['url']) }} as url,
    {{ json_extract_scalar('_airbyte_data', ['date'], ['date']) }} as {{ adapter.quote('date') }},
    {{ json_extract_scalar('_airbyte_data', ['host'], ['host']) }} as host,
    {{ json_extract_scalar('_airbyte_data', ['path'], ['path']) }} as {{ adapter.quote('path') }},
    {{ json_extract_scalar('_airbyte_data', ['uuid'], ['uuid']) }} as uuid,
    {{ json_extract_scalar('_airbyte_data', ['device'], ['device']) }} as device,
    {{ json_extract_scalar('_airbyte_data', ['medium'], ['medium']) }} as medium,
    {{ json_extract_scalar('_airbyte_data', ['source'], ['source']) }} as {{ adapter.quote('source') }},
    {{ json_extract_scalar('_airbyte_data', ['sessions'], ['sessions']) }} as sessions,
    {{ json_extract('table_alias', '_airbyte_data', ['url_hash']) }} as url_hash,
    {{ json_extract_scalar('_airbyte_data', ['is_golden'], ['is_golden']) }} as is_golden,
    {{ json_extract_scalar('_airbyte_data', ['new_users'], ['new_users']) }} as new_users,
    {{ json_extract_scalar('_airbyte_data', ['page_views'], ['page_views']) }} as page_views,
    {{ json_extract_scalar('_airbyte_data', ['data_source'], ['data_source']) }} as data_source,
    {{ json_extract_scalar('_airbyte_data', ['sync_timestamp'], ['sync_timestamp']) }} as sync_timestamp,
    {{ json_extract_scalar('_airbyte_data', ['engagedSessions'], ['engagedSessions']) }} as engagedsessions,
    {{ json_extract_scalar('_airbyte_data', ['session_duration'], ['session_duration']) }} as session_duration,
    {{ json_extract_scalar('_airbyte_data', ['goal_completions_all'], ['goal_completions_all']) }} as goal_completions_all,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('public', '_airbyte_raw_botify_visits_report') }} as table_alias
-- botify_visits_report
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

