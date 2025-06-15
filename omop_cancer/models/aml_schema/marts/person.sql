with source_data as (
    select * from {{ ref('stg__person') }}
)

select 
    person_id, person_source_value,
    year_of_birth, day_of_birth, month_of_birth,
    gender_source_value, gender_concept_id,
    birth_datetime,
    {{ generate_default_values() }}

from source_data