with src_1 as (
    select 
        *
    from {{ ref('stg__pre_encounter_a') }}  -- Assuming stg__pre_encounter_a is the staging table for pre-encounter data
),
src_2 as (
    select 
        *
    from {{ ref('stg__pre_encounter_b') }}  -- Assuming stg__pre_encounter_b is the staging table for pre-encounter data
),

unioned as (
    select * from src_1
    union all
    select * from src_2
)

select * from unioned