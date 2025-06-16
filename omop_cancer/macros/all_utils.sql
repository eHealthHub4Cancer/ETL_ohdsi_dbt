{% macro generate_plausible_date(date_col) %}
    -- This macro generates a death date based on the provided date column
    -- ensures the date is not in the future and is not null
    -- It also assumes the date_col is in a format that can be cast to date
    least(coalesce({{ date_col }}::date , current_date), current_date)
{% endmacro %}

{% macro validate_dates(date_col_1, date_col_2) %}
    -- This macro validates that the first date is not after the second date
    -- date_col_1 is expected to be same or above date_col_2
    greatest({{ date_col_1 }} , {{ date_col_2 }})

{% endmacro %}