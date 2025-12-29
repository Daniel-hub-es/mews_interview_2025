{% macro calculate_share(numerator, denominator, precision=2) %}
    cast(
        round(
            100.0 * {{ numerator }} / nullif({{ denominator }}, 0), 
            {{ precision }}
        ) as varchar
    ) || '%'
{% endmacro %}