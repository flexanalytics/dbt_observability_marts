{% macro convert_to_generic_type(column_name) %}
    {% set column_name = column_name | lower %}
    case
        when {{ column_name }} like '%varchar%' or {{ column_name }} like '%char%' then 'Text'
        when {{ column_name }} like '%numeric%' or {{ column_name }} like '%decimal%' or {{ column_name }} like '%number%' then 'Numeric'
        when {{ column_name }} in ('text', 'string', 'clob') then 'Text'
        when {{ column_name }} in ('integer', 'bigint', 'smallint', 'tinyint', 'mediumint', 'float', 'double', 'double precision', 'real', 'binary_float', 'binary_double', 'money') then 'Numeric'
        when {{ column_name }} in ('boolean', 'bit', 'bool', 'tinyint(1)') then 'Boolean'
        when {{ column_name }} in ('date', 'datetime', 'timestamp', 'timestamptz', 'time', 'smalldatetime', 'datetimeoffset', 'year', 'timestamp with time zone', 'timestamp without time zone', 'timestamp with local time zone') then 'Date/Time'
        when {{ column_name }} in ('blob', 'bytea', 'varbinary', 'binary', 'raw', 'long raw', 'image', 'byte') then 'Binary'
        when {{ column_name }} in ('uuid', 'uniqueidentifier', 'raw(16)', 'char(36)') then 'UUID'
        when {{ column_name }} in ('json', 'jsonb') then 'JSON'
        when {{ column_name }} in ('geometry', 'geography', 'sdo_geometry', 'point', 'linestring', 'polygon') then 'Spatial'
        else {{ column_name }}
    end
{% endmacro %}
