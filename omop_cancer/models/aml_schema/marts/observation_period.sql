-- logic adopted from https://ohdsi.github.io/CommonDataModel/ehrObsPeriods.html.

with src as (
    select 
        *
    from {{ ref('stg__observation_period') }}  -- Assuming stg__observation_period is the staging table for observation periods
),

final_1 as (
    select 
        person_id,
        {{ least_date('encounter_start_date', 'encounter_end_date') }} as start_date,
        {{ greatest_date('encounter_start_date', 'encounter_end_date') }} as end_date
    from src 
    where person_id is not null
    and (encounter_start_date is not null or encounter_end_date is not null)
),
-- mainly validating the birthdate. ensuring an observation period starts on or after the birth datetime.
validate_ob_birth as (
    select
        p1.person_id,
        {{ validate_dates('p1.start_date', 'p2.birth_datetime') }} as start_date,
        {{ generate_plausible_date('p1.end_date') }} as end_date
    from final_1 p1
    inner join {{ref('person') }} p2 on p1.person_id = p2.person_id
    where p2.person_id is not null
    and p1.start_date is not null
    and p1.end_date is not null
),
-- mainly validating the deathdate. Ensuring an observation period does not occur 
validate_ob_death as (
    select 
        p1.person_id,
        p1.start_date,
        {{ validate_death_date('p1.end_date', 'd.death_date') }} as end_date
    from validate_ob_birth p1
    left join {{ ref('death') }} d on p1.person_id = d.person_id
),
-- get previous dates for end date.
prev_dates as (
    select 
        person_id,
        start_date,
        end_date,
        LAG(end_date) OVER (PARTITION BY person_id ORDER BY start_date, end_date) as person_prev_end_date
    from validate_ob_death
),
-- get the flags
flag_gap_dates as (
    select *,
        CASE 
            WHEN person_prev_end_date is null THEN 1
            WHEN start_date - person_prev_end_date > {{etl_constants().obser_window}} THEN 1
            ELSE 0
        END as gap_days
    from prev_dates
),
-- sum over PARTITION
sum_gaps as (
    select *,
        SUM(gap_days) OVER (PARTITION BY person_id ORDER BY start_date ROWS UNBOUNDED PRECEDING) as gap_flag
    from flag_gap_dates
),
-- group the ids.

base_final as (
    SELECT
        person_id,
        gap_flag,
        MIN(start_date) as observation_period_start_date,
        MAX(end_date) as observation_period_end_date,
        {{ generate_observation_period_type_id() }}
    FROM sum_gaps
    GROUP BY person_id, gap_flag
),
-- observation period id needs to be unique per omop cdm guidelines.
-- force uniqueness by generating a surrogate key, and also using DISTINCT keyword.
final as (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['person_id', 'observation_period_start_date', 'observation_period_end_date', 'gap_flag']) }} as observation_period_id,
        person_id,
        observation_period_start_date,
        observation_period_end_date,
        observation_period_type_id
    FROM base_final
    ORDER BY person_id, observation_period_start_date, observation_period_end_date
)

select * from final