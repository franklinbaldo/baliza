import json
import os
import pytest
from src.baliza.main import ENDPOINTS_CONFIG

SPEC_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "docs", "openapi", "pncp_openapi.json")

@pytest.fixture(scope="module")
def openapi_spec():
    """Loads the OpenAPI specification from the local file."""
    if not os.path.exists(SPEC_FILE_PATH):
        pytest.fail(f"OpenAPI spec file not found at {SPEC_FILE_PATH}. Run scripts/fetch_openapi_spec.py first.")
    with open(SPEC_FILE_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

def test_all_endpoints_configured_exist_in_spec(openapi_spec):
    """Tests that every API path in ENDPOINTS_CONFIG is defined in the OpenAPI spec."""
    spec_paths = openapi_spec.get("paths", {})
    for endpoint_key, config in ENDPOINTS_CONFIG.items():
        assert config["api_path"] in spec_paths, \
            f"Endpoint '{endpoint_key}' with api_path '{config['api_path']}' not found in OpenAPI spec."

def test_endpoint_parameters_against_spec(openapi_spec):
    """
    Tests that configured parameters (date params, tamanhoPagina, required_params)
    for each endpoint are consistent with the OpenAPI specification.
    Also checks the 'tamanhoPagina' value against the spec's maximum.
    """
    spec_paths = openapi_spec.get("paths", {})

    for endpoint_key, config in ENDPOINTS_CONFIG.items():
        api_path = config["api_path"]
        assert api_path in spec_paths, \
            f"Endpoint '{endpoint_key}' api_path '{api_path}' not found in spec. Cannot validate params."

        # Assuming all our configured endpoints use the GET method
        path_spec = spec_paths[api_path].get("get")
        assert path_spec is not None, \
            f"No GET method defined for path '{api_path}' in OpenAPI spec for endpoint '{endpoint_key}'."

        spec_params = path_spec.get("parameters", [])
        spec_param_names = [p["name"] for p in spec_params]

        # Check date parameters
        assert config["date_param_initial"] in spec_param_names, \
            f"Configured 'date_param_initial' ('{config['date_param_initial']}') for endpoint '{endpoint_key}' not found in spec for path '{api_path}'."
        assert config["date_param_final"] in spec_param_names, \
            f"Configured 'date_param_final' ('{config['date_param_final']}') for endpoint '{endpoint_key}' not found in spec for path '{api_path}'."

        # Check tamanhoPagina parameter name
        assert "tamanhoPagina" in spec_param_names, \
            f"'tamanhoPagina' parameter not found in spec for path '{api_path}' (endpoint '{endpoint_key}')."

        # Validate configured tamanhoPagina value against spec's maximum, if defined
        tamanho_pagina_spec = next((p for p in spec_params if p["name"] == "tamanhoPagina"), None)
        if tamanho_pagina_spec and "schema" in tamanho_pagina_spec and "maximum" in tamanho_pagina_spec["schema"]:
            max_page_size_spec = tamanho_pagina_spec["schema"]["maximum"]
            configured_page_size = config["tamanhoPagina"]
            assert configured_page_size <= max_page_size_spec, \
                f"Configured 'tamanhoPagina' ({configured_page_size}) for endpoint '{endpoint_key}' exceeds spec maximum ({max_page_size_spec}) for path '{api_path}'."

        # Check other required parameters
        for req_param_name in config.get("required_params", {}).keys():
            assert req_param_name in spec_param_names, \
                f"Configured 'required_params' key ('{req_param_name}') for endpoint '{endpoint_key}' not found in spec for path '{api_path}'."

def test_required_params_in_spec_are_handled(openapi_spec):
    """
    Checks if parameters marked as 'required: true' in the spec (for GET operations)
    are accounted for in our ENDPOINTS_CONFIG (either as date params, 'tamanhoPagina', or in 'required_params').
    This helps identify if we are missing any mandatory parameters.
    """
    spec_paths = openapi_spec.get("paths", {})

    for endpoint_key, config in ENDPOINTS_CONFIG.items():
        api_path = config["api_path"]
        if api_path not in spec_paths or spec_paths[api_path].get("get") is None:
            continue # Existence already checked in another test

        path_spec_get = spec_paths[api_path]["get"]
        spec_params = path_spec_get.get("parameters", [])

        configured_param_names = {
            config["date_param_initial"],
            config["date_param_final"],
            "tamanhoPagina", # This is for the value, the param name is also 'tamanhoPagina'
            "pagina" # Common pagination param, usually handled by the fetching loop
        }
        configured_param_names.update(config.get("required_params", {}).keys())

        # Parameters like 'codigoModalidadeContratacao' are intentionally omitted for some endpoints,
        # relying on API leniency. This test will highlight them if they are strictly required by spec.
        # We might need to refine this test or acknowledge these known omissions.
        # For now, it serves as a good check.

        # List of parameters that are required in the spec but intentionally not sent for specific endpoints,
        # relying on API leniency or because they would overly restrict the query for a bulk download.
        intentionally_omitted_for_endpoint = {
            "contratacoes_publicacao": ["codigoModalidadeContratacao"],
            "contratacoes_atualizacao": ["codigoModalidadeContratacao"],
            # Add other endpoint_key: [param_names] if needed
        }

        for spec_param in spec_params:
            param_name = spec_param["name"]
            if spec_param.get("required", False) and param_name not in configured_param_names:
                # Check if this parameter is intentionally omitted for this specific endpoint
                if endpoint_key in intentionally_omitted_for_endpoint and \
                   param_name in intentionally_omitted_for_endpoint[endpoint_key]:
                    # Optionally, log this as a known omission rather than failing
                    print(f"INFO: Mandatory parameter '{param_name}' for endpoint '{endpoint_key}' is intentionally omitted.")
                    continue

                assert False, (
                    f"Mandatory parameter '{param_name}' in spec for endpoint '{endpoint_key}' (path '{api_path}') "
                    f"is not explicitly handled in ENDPOINTS_CONFIG (date_params, tamanhoPagina, required_params, or 'pagina')."
                )

# It might be useful to also test if all GET endpoints from the spec that look like
# bulk data endpoints (e.g., accept date range and pagination) are covered in our config.
# This is a more complex "coverage" test and is out of scope for the current task.
