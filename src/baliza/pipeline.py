import dlt
import requests
from datetime import date, timedelta

BASE_URL = "https://pncp.gov.br/api/consulta/v1"

@dlt.resource(write_disposition="merge")
def contratacoes(
    start_date: date,
    end_date: date,
    api_key: str = dlt.secrets.value
):
    date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

    for single_date in date_range:
        page = 1
        while True:
            params = {
                "dataInicial": single_date.strftime('%Y%m%d'),
                "dataFinal": single_date.strftime('%Y%m%d'),
                "pagina": page,
                "tamanhoPagina": 500,
            }
            headers = {"Authorization": f"Bearer {api_key}"}
            response = requests.get(f"{BASE_URL}/contratacoes", params=params, headers=headers)
            response.raise_for_status()
            data = response.json()

            if not data.get("data"):
                break

            yield from data["data"]

            if not data.get("paginacao", {}).get("proximaPagina"):
                break

            page += 1

@dlt.source
def pncp_source(
    start_date: date,
    end_date: date,
    api_key: str = dlt.secrets.value
):
    return contratacoes(start_date, end_date, api_key)

def run_pipeline(start_date: date, end_date: date):
    """
    Runs the dlt pipeline to extract data from the PNCP API and load it into DuckDB.
    """
    pipeline = dlt.pipeline(
        pipeline_name="pncp",
        destination="duckdb",
        dataset_name="pncp_data",
    )
    load_info = pipeline.run(pncp_source(start_date, end_date))
    print(load_info)

if __name__ == "__main__":
    # Example run for a single day
    today = date.today()
    run_pipeline(today, today)
