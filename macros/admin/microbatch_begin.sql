{% macro microbatch_begin() %}

    {%- if target.name in ("default","dev") -%} {{ (modules.datetime.datetime.now() - modules.datetime.timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0) }}
    {%- elif target.name == "CI" -%} {{ (modules.datetime.datetime.now() - modules.datetime.timedelta(days=14)).replace(hour=0, minute=0, second=0, microsecond=0) }}
    {%- else -%} {{modules.datetime.datetime(2018, 1, 1, 0, 0, 0)}}
    {%- endif -%}
{% endmacro %}
