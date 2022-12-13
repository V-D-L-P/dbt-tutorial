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
    cast(uuid as {{ dbt_utils.type_string() }}) as uuid,
    cast(hostname as {{ dbt_utils.type_string() }}) as hostname,
    cast(newusers as {{ dbt_utils.type_bigint() }}) as newusers,
    cast(sessions as {{ dbt_utils.type_bigint() }}) as sessions,
    url_hash,
    {{ cast_to_boolean('is_golden') }} as is_golden,
    cast(conversions as {{ dbt_utils.type_float() }}) as conversions,
    cast(landingpage as {{ dbt_utils.type_string() }}) as landingpage,
    cast(sessionmedium as {{ dbt_utils.type_string() }}) as sessionmedium,
    cast(sessionsource as {{ dbt_utils.type_string() }}) as sessionsource,
    cast(devicecategory as {{ dbt_utils.type_string() }}) as devicecategory,
    cast(engagedsessions as {{ dbt_utils.type_bigint() }}) as engagedsessions,
    cast(screenpageviews as {{ dbt_utils.type_bigint() }}) as screenpageviews,
    cast(userengagementduration as {{ dbt_utils.type_float() }}) as userengagementduration,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('botify_visits_report_ab1') }}
-- botify_visits_report
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

