{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('botify_visits_report_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'url',
        adapter.quote('date'),
        'uuid',
        'hostname',
        'newusers',
        'sessions',
        'url_hash',
        boolean_to_string('is_golden'),
        'conversions',
        'landingpage',
        'sessionmedium',
        'sessionsource',
        'devicecategory',
        'engagedsessions',
        'screenpageviews',
        'userengagementduration',
    ]) }} as _airbyte_botify_visits_report_hashid,
    tmp.*
from {{ ref('botify_visits_report_ab2') }} tmp
-- botify_visits_report
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

