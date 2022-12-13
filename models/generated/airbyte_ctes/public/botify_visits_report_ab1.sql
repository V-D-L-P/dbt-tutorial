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
    {{ json_extract_scalar('_airbyte_data', ['uuid'], ['uuid']) }} as uuid,
    {{ json_extract_scalar('_airbyte_data', ['hostName'], ['hostName']) }} as hostname,
    {{ json_extract_scalar('_airbyte_data', ['newUsers'], ['newUsers']) }} as newusers,
    {{ json_extract_scalar('_airbyte_data', ['sessions'], ['sessions']) }} as sessions,
    {{ json_extract('table_alias', '_airbyte_data', ['url_hash']) }} as url_hash,
    {{ json_extract_scalar('_airbyte_data', ['is_golden'], ['is_golden']) }} as is_golden,
    {{ json_extract_scalar('_airbyte_data', ['conversions'], ['conversions']) }} as conversions,
    {{ json_extract_scalar('_airbyte_data', ['landingPage'], ['landingPage']) }} as landingpage,
    {{ json_extract_scalar('_airbyte_data', ['sessionMedium'], ['sessionMedium']) }} as sessionmedium,
    {{ json_extract_scalar('_airbyte_data', ['sessionSource'], ['sessionSource']) }} as sessionsource,
    {{ json_extract_scalar('_airbyte_data', ['deviceCategory'], ['deviceCategory']) }} as devicecategory,
    {{ json_extract_scalar('_airbyte_data', ['engagedSessions'], ['engagedSessions']) }} as engagedsessions,
    {{ json_extract_scalar('_airbyte_data', ['screenPageViews'], ['screenPageViews']) }} as screenpageviews,
    {{ json_extract_scalar('_airbyte_data', ['userEngagementDuration'], ['userEngagementDuration']) }} as userengagementduration,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('public', '_airbyte_raw_botify_visits_report') }} as table_alias
-- botify_visits_report
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

