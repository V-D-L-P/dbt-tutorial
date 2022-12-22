{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('botify_visits_report_ab1') }}
select
    cast(url as {{ dbt_utils.type_string() }}) as url,
    cast({{ empty_string_to_null(adapter.quote('date')) }} as {{ type_date() }}) as {{ adapter.quote('date') }},
    cast(host as {{ dbt_utils.type_string() }}) as host,
    cast({{ adapter.quote('path') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('path') }},
    cast(uuid as {{ dbt_utils.type_string() }}) as uuid,
    cast(device as {{ dbt_utils.type_string() }}) as device,
    cast(medium as {{ dbt_utils.type_string() }}) as medium,
    cast({{ adapter.quote('source') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('source') }},
    cast(sessions as {{ dbt_utils.type_bigint() }}) as sessions,
    url_hash,
    {{ cast_to_boolean('is_golden') }} as is_golden,
    cast(new_users as {{ dbt_utils.type_bigint() }}) as new_users,
    cast(page_views as {{ dbt_utils.type_bigint() }}) as page_views,
    cast(data_source as {{ dbt_utils.type_string() }}) as data_source,
    cast(sync_timestamp as {{ dbt_utils.type_string() }}) as sync_timestamp,
    cast(engagedsessions as {{ dbt_utils.type_bigint() }}) as engagedsessions,
    cast(session_duration as {{ dbt_utils.type_float() }}) as session_duration,
    cast(goal_completions_all as {{ dbt_utils.type_float() }}) as goal_completions_all,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('botify_visits_report_ab1') }}
-- botify_visits_report
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

