{% macro registers_decoder(lower_register, higher_register, scale_factor) %}
    (CASE
        WHEN (( {{lower_register}} << 16 ) | {{ higher_register }}) > 2147483647
            THEN (( {{lower_register}} << 16 ) | {{ higher_register }}) - 4294967296
        ELSE (( {{lower_register}} << 16 ) | {{ higher_register }})
    END) / {{scale_factor}}
{% endmacro %}