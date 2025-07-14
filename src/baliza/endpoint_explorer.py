#!/usr/bin/env python3
"""
PNCP Endpoint Explorer
Comprehensive tool to test and extract data from all available PNCP API endpoints
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional
import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, TaskID

from .pncp_client import PNCPClient
from .api_pncp_consulta_client.api.contrato_empenho import (
    consultar_contratos_por_data_publicacao,
    consultar_contratos_por_data_atualizacao_global
)

console = Console()

class PNCPEndpointExplorer:
    """Comprehensive explorer for all PNCP API endpoints."""
    
    def __init__(self, base_url: str = "https://pncp.gov.br/api/consulta"):
        self.client = PNCPClient(base_url=base_url)
        self.base_url = base_url
        self.results_dir = Path("data/endpoint_analysis")
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
    def test_all_endpoints(self, test_date: str = "20240710") -> Dict[str, Any]:
        """Test all available PNCP endpoints with a sample date.
        
        Args:
            test_date: Date in YYYYMMDD format to test with
            
        Returns:
            Comprehensive results from all endpoints
        """
        console.print(Panel(f"🔍 PNCP API Endpoint Comprehensive Analysis", style="bold blue"))
        console.print(f"📅 Test Date: {test_date}")
        console.print(f"🌐 Base URL: {self.base_url}")
        
        all_results = {
            "test_date": test_date,
            "base_url": self.base_url,
            "endpoints": {}
        }
        
        with Progress() as progress:
            main_task = progress.add_task("[green]Testing endpoints...", total=8)
            
            # 1. Contratos por Data de Publicação  
            progress.update(main_task, description="[green]Testing Contratos (Publication Date)...")
            all_results["endpoints"]["contratos_publicacao"] = self._test_contratos_publicacao(test_date, progress)
            progress.advance(main_task)
            
            # 2. Contratos por Data de Atualização
            progress.update(main_task, description="[green]Testing Contratos (Update Date)...")
            all_results["endpoints"]["contratos_atualizacao"] = self._test_contratos_atualizacao(test_date, progress)
            progress.advance(main_task)
            
            # 3. Contratações por Data de Publicação
            progress.update(main_task, description="[green]Testing Contratações (Publication Date)...")
            all_results["endpoints"]["contratacoes_publicacao"] = self._test_contratacoes_publicacao(test_date, progress)
            progress.advance(main_task)
            
            # 4. Contratações por Data de Atualização
            progress.update(main_task, description="[green]Testing Contratações (Update Date)...")
            all_results["endpoints"]["contratacoes_atualizacao"] = self._test_contratacoes_atualizacao(test_date, progress)
            progress.advance(main_task)
            
            # 5. Contratações - Período de Recebimento de Propostas
            progress.update(main_task, description="[green]Testing Contratações (Proposal Period)...")
            all_results["endpoints"]["contratacoes_propostas"] = self._test_contratacoes_propostas(test_date, progress)
            progress.advance(main_task)
            
            # 6. Atas de Registro de Preços por Período
            progress.update(main_task, description="[green]Testing Atas (Period)...")
            all_results["endpoints"]["atas_periodo"] = self._test_atas_periodo(test_date, progress)
            progress.advance(main_task)
            
            # 7. Atas de Registro de Preços por Data de Atualização
            progress.update(main_task, description="[green]Testing Atas (Update Date)...")
            all_results["endpoints"]["atas_atualizacao"] = self._test_atas_atualizacao(test_date, progress)
            progress.advance(main_task)
            
            # 8. Save comprehensive results
            progress.update(main_task, description="[green]Saving results...")
            self._save_results(all_results, test_date)
            progress.advance(main_task)
        
        self._print_summary(all_results)
        return all_results
    
    def _test_contratos_publicacao(self, test_date: str, progress: Optional[Progress] = None) -> Dict[str, Any]:
        """Test contratos endpoint by publication date."""
        endpoint_info = {
            "name": "Contratos por Data de Publicação",
            "path": "/v1/contratos",
            "description": "Contracts by publication date",
            "status": "unknown",
            "sample_data": None,
            "total_records": 0,
            "sample_fields": [],
            "error": None
        }
        
        try:
            # Use existing wrapper method
            data = self.client.fetch_contratos_data(
                data_inicial=test_date,
                data_final=test_date,
                pagina=1,
                tamanho_pagina=10
            )
            
            endpoint_info["status"] = "success"
            endpoint_info["total_records"] = data.get("totalRegistros", 0)
            
            if data.get("data") and len(data["data"]) > 0:
                endpoint_info["sample_data"] = data["data"][:3]  # First 3 records
                endpoint_info["sample_fields"] = list(data["data"][0].keys())
                
        except Exception as e:
            endpoint_info["status"] = "error"
            endpoint_info["error"] = str(e)
            
        return endpoint_info
    
    def _test_contratos_atualizacao(self, test_date: str, progress: Optional[Progress] = None) -> Dict[str, Any]:
        """Test contratos endpoint by update date."""
        endpoint_info = {
            "name": "Contratos por Data de Atualização",
            "path": "/v1/contratos/atualizacao",
            "description": "Contracts by update date",
            "status": "unknown",
            "sample_data": None,
            "total_records": 0,
            "sample_fields": [],
            "error": None
        }
        
        try:
            response = consultar_contratos_por_data_atualizacao_global.sync_detailed(
                client=self.client.client,
                data_inicial=test_date,
                data_final=test_date,
                pagina=1,
                tamanho_pagina=10
            )
            
            if response.status_code == 200:
                data = self._parse_response_content(response)
                endpoint_info["status"] = "success"
                endpoint_info["total_records"] = data.get("totalRegistros", 0)
                
                if data.get("data") and len(data["data"]) > 0:
                    endpoint_info["sample_data"] = data["data"][:3]
                    endpoint_info["sample_fields"] = list(data["data"][0].keys())
            elif response.status_code == 204:
                endpoint_info["status"] = "no_data"
            else:
                endpoint_info["status"] = "error"
                endpoint_info["error"] = f"HTTP {response.status_code}"
                
        except Exception as e:
            endpoint_info["status"] = "error"
            endpoint_info["error"] = str(e)
            
        return endpoint_info
    
    def _test_contratacoes_publicacao(self, test_date: str, progress: Optional[Progress] = None) -> Dict[str, Any]:
        """Test contratações endpoint by publication date."""
        endpoint_info = {
            "name": "Contratações por Data de Publicação",
            "path": "/v1/contratacoes/publicacao",
            "description": "Procurements by publication date",
            "status": "unknown",
            "sample_data": None,
            "total_records": 0,
            "sample_fields": [],
            "error": None
        }
        
        try:
            response = consultar_contratacao_por_data_de_publicacao.sync_detailed(
                client=self.client.client,
                data_inicial=test_date,
                data_final=test_date,
                pagina=1,
                tamanho_pagina=10
            )
            
            if response.status_code == 200:
                data = self._parse_response_content(response)
                endpoint_info["status"] = "success"
                endpoint_info["total_records"] = data.get("totalRegistros", 0)
                
                if data.get("data") and len(data["data"]) > 0:
                    endpoint_info["sample_data"] = data["data"][:3]
                    endpoint_info["sample_fields"] = list(data["data"][0].keys())
            elif response.status_code == 204:
                endpoint_info["status"] = "no_data"
            else:
                endpoint_info["status"] = "error"
                endpoint_info["error"] = f"HTTP {response.status_code}"
                
        except Exception as e:
            endpoint_info["status"] = "error" 
            endpoint_info["error"] = str(e)
            
        return endpoint_info
    
    def _test_contratacoes_atualizacao(self, test_date: str, progress: Optional[Progress] = None) -> Dict[str, Any]:
        """Test contratações endpoint by update date.""" 
        endpoint_info = {
            "name": "Contratações por Data de Atualização",
            "path": "/v1/contratacoes/atualizacao", 
            "description": "Procurements by update date",
            "status": "unknown",
            "sample_data": None,
            "total_records": 0,
            "sample_fields": [],
            "error": None
        }
        
        try:
            response = consultar_contratacao_por_data_ultima_atualizacao.sync_detailed(
                client=self.client.client,
                data_inicial=test_date,
                data_final=test_date,
                pagina=1,
                tamanho_pagina=10
            )
            
            if response.status_code == 200:
                data = self._parse_response_content(response)
                endpoint_info["status"] = "success"
                endpoint_info["total_records"] = data.get("totalRegistros", 0)
                
                if data.get("data") and len(data["data"]) > 0:
                    endpoint_info["sample_data"] = data["data"][:3]
                    endpoint_info["sample_fields"] = list(data["data"][0].keys())
            elif response.status_code == 204:
                endpoint_info["status"] = "no_data"
            else:
                endpoint_info["status"] = "error"
                endpoint_info["error"] = f"HTTP {response.status_code}"
                
        except Exception as e:
            endpoint_info["status"] = "error"
            endpoint_info["error"] = str(e)
            
        return endpoint_info
    
    def _test_contratacoes_propostas(self, test_date: str, progress: Optional[Progress] = None) -> Dict[str, Any]:
        """Test contratações endpoint for proposal reception period."""
        endpoint_info = {
            "name": "Contratações - Período de Recebimento de Propostas",
            "path": "/v1/contratacoes/proposta",
            "description": "Procurements in proposal reception period", 
            "status": "unknown",
            "sample_data": None,
            "total_records": 0,
            "sample_fields": [],
            "error": None
        }
        
        try:
            response = consultar_contratacao_periodo_recebimento_propostas.sync_detailed(
                client=self.client.client,
                data_inicial=test_date,
                data_final=test_date,
                pagina=1,
                tamanho_pagina=10
            )
            
            if response.status_code == 200:
                data = self._parse_response_content(response)
                endpoint_info["status"] = "success"
                endpoint_info["total_records"] = data.get("totalRegistros", 0)
                
                if data.get("data") and len(data["data"]) > 0:
                    endpoint_info["sample_data"] = data["data"][:3]
                    endpoint_info["sample_fields"] = list(data["data"][0].keys())
            elif response.status_code == 204:
                endpoint_info["status"] = "no_data"
            else:
                endpoint_info["status"] = "error"
                endpoint_info["error"] = f"HTTP {response.status_code}"
                
        except Exception as e:
            endpoint_info["status"] = "error"
            endpoint_info["error"] = str(e)
            
        return endpoint_info
    
    def _test_atas_periodo(self, test_date: str, progress: Optional[Progress] = None) -> Dict[str, Any]:
        """Test atas endpoint by period."""
        endpoint_info = {
            "name": "Atas de Registro de Preços por Período",
            "path": "/v1/atas",
            "description": "Price registration records by period",
            "status": "unknown", 
            "sample_data": None,
            "total_records": 0,
            "sample_fields": [],
            "error": None
        }
        
        try:
            response = consultar_ata_registro_preco_periodo.sync_detailed(
                client=self.client.client,
                data_inicial=test_date,
                data_final=test_date,
                pagina=1,
                tamanho_pagina=10
            )
            
            if response.status_code == 200:
                data = self._parse_response_content(response)
                endpoint_info["status"] = "success"
                endpoint_info["total_records"] = data.get("totalRegistros", 0)
                
                if data.get("data") and len(data["data"]) > 0:
                    endpoint_info["sample_data"] = data["data"][:3]
                    endpoint_info["sample_fields"] = list(data["data"][0].keys())
            elif response.status_code == 204:
                endpoint_info["status"] = "no_data"
            else:
                endpoint_info["status"] = "error"
                endpoint_info["error"] = f"HTTP {response.status_code}"
                
        except Exception as e:
            endpoint_info["status"] = "error"
            endpoint_info["error"] = str(e)
            
        return endpoint_info
    
    def _test_atas_atualizacao(self, test_date: str, progress: Optional[Progress] = None) -> Dict[str, Any]:
        """Test atas endpoint by update date."""
        endpoint_info = {
            "name": "Atas de Registro de Preços por Data de Atualização",
            "path": "/v1/atas/atualizacao",
            "description": "Price registration records by update date",
            "status": "unknown",
            "sample_data": None,
            "total_records": 0, 
            "sample_fields": [],
            "error": None
        }
        
        try:
            response = consultar_ata_registro_preco_data_atualizacao.sync_detailed(
                client=self.client.client,
                data_inicial=test_date,
                data_final=test_date,
                pagina=1,
                tamanho_pagina=10
            )
            
            if response.status_code == 200:
                data = self._parse_response_content(response)
                endpoint_info["status"] = "success"
                endpoint_info["total_records"] = data.get("totalRegistros", 0)
                
                if data.get("data") and len(data["data"]) > 0:
                    endpoint_info["sample_data"] = data["data"][:3]
                    endpoint_info["sample_fields"] = list(data["data"][0].keys())
            elif response.status_code == 204:
                endpoint_info["status"] = "no_data"
            else:
                endpoint_info["status"] = "error"
                endpoint_info["error"] = f"HTTP {response.status_code}"
                
        except Exception as e:
            endpoint_info["status"] = "error"
            endpoint_info["error"] = str(e)
            
        return endpoint_info
        
    def _parse_response_content(self, response) -> Dict[str, Any]:
        """Parse response content from OpenAPI client response."""
        if response.parsed:
            return response.parsed
        elif isinstance(response.content, bytes):
            try:
                return json.loads(response.content.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                return {"data": [], "totalRegistros": 0, "totalPaginas": 0}
        elif isinstance(response.content, dict):
            return response.content
        else:
            return {"data": [], "totalRegistros": 0, "totalPaginas": 0}
    
    def _save_results(self, results: Dict[str, Any], test_date: str):
        """Save comprehensive results to JSON file."""
        output_file = self.results_dir / f"pncp_endpoint_analysis_{test_date}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
            
        console.print(f"💾 Results saved to: {output_file}")
        
    def _print_summary(self, results: Dict[str, Any]):
        """Print a beautiful summary table of all endpoint results."""
        table = Table(title="🔍 PNCP API Endpoint Analysis Summary")
        
        table.add_column("Endpoint", style="cyan", no_wrap=True)
        table.add_column("Status", style="green")
        table.add_column("Total Records", justify="right", style="yellow")
        table.add_column("Sample Fields", style="blue")
        table.add_column("Error", style="red")
        
        for endpoint_key, endpoint_data in results["endpoints"].items():
            status_emoji = {
                "success": "✅",
                "no_data": "📭", 
                "error": "❌",
                "unknown": "❓"
            }.get(endpoint_data["status"], "❓")
            
            status_text = f"{status_emoji} {endpoint_data['status']}"
            records_text = str(endpoint_data.get("total_records", 0))
            fields_text = str(len(endpoint_data.get("sample_fields", [])))
            error_text = endpoint_data.get("error", "") or ""
            
            table.add_row(
                endpoint_data["name"],
                status_text,
                records_text,
                fields_text,
                error_text[:50] + "..." if len(error_text) > 50 else error_text
            )
        
        console.print(table)
        
        # Summary stats
        total_endpoints = len(results["endpoints"])
        successful_endpoints = sum(1 for ep in results["endpoints"].values() if ep["status"] == "success")
        total_records = sum(ep.get("total_records", 0) for ep in results["endpoints"].values())
        
        console.print(Panel(
            f"📊 Summary: {successful_endpoints}/{total_endpoints} endpoints successful\n"
            f"📄 Total records found: {total_records:,}",
            title="Analysis Complete",
            style="bold green"
        ))


# CLI command
def explore_endpoints(
    test_date: str = typer.Option("20240710", help="Date to test in YYYYMMDD format"),
    save_samples: bool = typer.Option(True, help="Save sample data to files")
):
    """Explore and test all PNCP API endpoints comprehensively.""" 
    explorer = PNCPEndpointExplorer()
    results = explorer.test_all_endpoints(test_date)
    
    if save_samples:
        console.print("💾 Sample data saved for analysis")


if __name__ == "__main__":
    typer.run(explore_endpoints)