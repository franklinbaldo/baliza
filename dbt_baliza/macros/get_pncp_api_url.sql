{% macro get_pncp_api_url(endpoint_name, start_date_var='start_date', end_date_var='end_date') %}
    {% set base_url = 'https://pncp.gov.br/api/consulta/v1/' %}
    {% set endpoint_path = var('endpoints')[endpoint_name] %}
    {% set start_date = var(start_date_var) %}
    {% set end_date = var(end_date_var) %}
    {% set formatted_start_date = start_date | replace('-', '') %}
    {% set formatted_end_date = end_date | replace('-', '') %}

    {{ return(base_url ~ endpoint_path ~ '?dataInicial=' ~ formatted_start_date ~ '&dataFinal=' ~ formatted_end_date ~ '&tamanhoPagina=500') }}
{% endmacro %}
