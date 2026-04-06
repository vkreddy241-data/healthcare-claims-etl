-- Macro: pii_mask
-- SHA-256 hashes a PII column for HIPAA Safe Harbor compliance.
-- Usage: {{ pii_mask('member_id') }}

{% macro pii_mask(column_name) %}
    sha2(cast({{ column_name }} as varchar), 256)
{% endmacro %}
