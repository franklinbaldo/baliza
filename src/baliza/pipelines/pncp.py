import dlt
from ..legacy.enums import PncpEndpoint

@dlt.source
def pncp_source(endpoints: list[PncpEndpoint] = None):
    if endpoints is None:
        endpoints = list(PncpEndpoint)

    for endpoint in endpoints:
        yield dlt.resource(
            _get_endpoint_data,
            name=endpoint.name,
        )(endpoint)

def _get_endpoint_data(endpoint: PncpEndpoint):
    # This is a placeholder for the actual data fetching logic
    yield {"endpoint": endpoint.name, "data": "some_data"}
