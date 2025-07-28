"""
Testes para o pipeline Baliza PNCP.
Testes smoke que verificam se a configuração e API ainda são compatíveis.
"""

import pytest
from pathlib import Path
import yaml
import dlt
from unittest.mock import patch, MagicMock

# Importa as funções do pipeline
from src.baliza.pipeline import (
    baliza_source,
    _get_resources_by_type,
    _process_resource_config,
    _requires_modalidade,
    _generate_modalidade_resources,
    run_pipeline,
    ENDPOINTS_REQUIRING_MODALIDADE
)


class TestPipelineConfig:
    """Testes de configuração do pipeline."""
    
    def test_yaml_config_exists_and_valid(self):
        """Testa se o arquivo de configuração YAML existe e é válido."""
        config_path = Path("config/pncp_resources.yaml")
        assert config_path.exists(), "Arquivo de configuração YAML não encontrado"
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # Verifica estrutura básica
        assert "sync_resources" in config
        assert "backfill_resources" in config
        assert "specialized_resources" in config
        
        # Verifica que existem recursos em cada seção
        assert len(config["sync_resources"]) > 0
        assert len(config["backfill_resources"]) > 0
        
    def test_get_resources_by_type(self):
        """Testa seleção de recursos por tipo."""
        mock_config = {
            "sync_resources": [{"name": "sync1"}],
            "backfill_resources": [{"name": "backfill1"}],
            "specialized_resources": [{"name": "special1"}]
        }
        
        sync_resources = _get_resources_by_type(mock_config, "sync")
        assert len(sync_resources) == 1
        assert sync_resources[0]["name"] == "sync1"
        
        backfill_resources = _get_resources_by_type(mock_config, "backfill")
        assert len(backfill_resources) == 1
        assert backfill_resources[0]["name"] == "backfill1"
        
        # Testa tipo inválido (deve retornar sync por padrão)
        invalid_resources = _get_resources_by_type(mock_config, "invalid")
        assert len(invalid_resources) == 1
        assert invalid_resources[0]["name"] == "sync1"
    
    def test_process_resource_config(self):
        """Testa processamento de configuração de resource."""
        mock_resource = {
            "name": "test_resource",
            "endpoint": {
                "params": {
                    "pagina": 1
                }
            },
            "incremental": {
                "cursor_path": "dataAtualizacao",
                "initial_value": "2021-01-01T00:00:00Z",
                "lag_days": 1
            }
        }
        
        processed = _process_resource_config(mock_resource)
        
        # Verifica que não aplicamos filtros de modalidade (backup completo)
        
        # Verifica que paginador foi aplicado
        assert "paginator" in processed["endpoint"]
        
        # Verifica que placeholders incrementais foram aplicados
        assert processed["endpoint"]["params"]["dataInicial"] == "{incremental.start_date}"
        assert processed["endpoint"]["params"]["dataFinal"] == "{incremental.end_date}"
        
        # Verifica que seção incremental foi removida
        assert "incremental" not in processed


class TestModalidadeStrategy:
    """Testes da estratégia multi-modalidade."""
    
    def test_requires_modalidade_detection(self):
        """Testa detecção correta de endpoints que requerem modalidade."""
        # Endpoints que REQUEREM modalidade
        assert _requires_modalidade("contratacoes_publicacao") == True
        assert _requires_modalidade("contratacoes_atualizacao") == True
        
        # Endpoints que NÃO REQUEREM modalidade  
        assert _requires_modalidade("contratos") == False
        assert _requires_modalidade("contratos_atualizacao") == False
        assert _requires_modalidade("atas") == False
        assert _requires_modalidade("atas_atualizacao") == False
        assert _requires_modalidade("pca_usuario") == False
        assert _requires_modalidade("instrumentos_cobranca") == False
        
        # Testa com suffixos de modalidade (devem ser ignorados)
        assert _requires_modalidade("contratacoes_publicacao_mod6") == True
        assert _requires_modalidade("contratos_mod1") == False
    
    def test_generate_modalidade_resources(self):
        """Testa geração de 13 resources para cada modalidade."""
        base_resource = {
            "name": "contratacoes_publicacao",
            "table_name": "contratacao_publicacao", 
            "endpoint": {
                "path": "/v1/contratacoes/publicacao",
                "params": {
                    "dataInicial": "2024-01-01",
                    "pagina": 1
                }
            }
        }
        
        modalidade_resources = _generate_modalidade_resources(base_resource)
        
        # Verifica que foram gerados 13 resources (uma para cada modalidade)
        assert len(modalidade_resources) == 13
        
        # Verifica nomes únicos
        names = [r["name"] for r in modalidade_resources]
        assert len(set(names)) == 13  # Todos únicos
        
        # Verifica que modalidades 1-13 estão presentes
        expected_names = [f"contratacoes_publicacao_mod{i}" for i in range(1, 14)]
        assert sorted(names) == sorted(expected_names)
        
        # Verifica que cada resource tem modalidade diferente
        modalidades = [r["endpoint"]["params"]["codigoModalidadeContratacao"] for r in modalidade_resources]
        assert sorted(modalidades) == list(range(1, 14))
        
        # Verifica que outros parâmetros foram preservados
        for resource in modalidade_resources:
            assert resource["table_name"] == "contratacao_publicacao"
            assert resource["endpoint"]["params"]["dataInicial"] == "2024-01-01"
            assert resource["endpoint"]["params"]["pagina"] == 1
    
    def test_endpoints_requiring_modalidade_list(self):
        """Testa que a lista de endpoints requerentes está correta."""
        # Baseado na análise dos scripts, apenas estes dois requerem modalidade
        expected = {"contratacoes_publicacao", "contratacoes_atualizacao"}
        assert ENDPOINTS_REQUIRING_MODALIDADE == expected


class TestPipelineSource:
    """Testes do source DLT."""
    
    @patch('src.baliza.pipeline.rest_api_source')
    @patch('builtins.open')
    @patch('yaml.safe_load')
    def test_baliza_source_creation(self, mock_yaml_load, mock_open, mock_rest_api_source):
        """Testa criação do source Baliza."""
        # Mock da configuração YAML
        mock_config = {
            "sync_resources": [{
                "name": "test_resource",
                "endpoint": {
                    "params": {"pagina": 1}
                }
            }]
        }
        mock_yaml_load.return_value = mock_config
        
        # Mock do rest_api_source
        mock_rest_api_source.return_value = MagicMock()
        
        # Testa criação do source
        source = baliza_source(
            base_url="https://test.api.com",
            resource_type="sync"
        )
        
        # Verifica que rest_api_source foi chamado
        mock_rest_api_source.assert_called_once()
        
        # Verifica estrutura da configuração passada
        call_args = mock_rest_api_source.call_args[0][0]
        assert "client" in call_args
        assert call_args["client"]["base_url"] == "https://test.api.com"
        assert "resources" in call_args
        # Note: pode ter múltiplos resources se algum requerer modalidade
        assert len(call_args["resources"]) >= 1


class TestPipelineDryRun:
    """Testes de dry run do pipeline."""
    
    @patch('src.baliza.pipeline.baliza_source')
    @patch('dlt.pipeline')
    def test_pipeline_dry_run_sync(self, mock_dlt_pipeline, mock_baliza_source):
        """Testa execução dry run do pipeline em modo sync."""
        # Mock do source
        mock_source = MagicMock()
        mock_baliza_source.return_value = mock_source
        
        # Mock do pipeline
        mock_pipeline_instance = MagicMock()
        mock_dlt_pipeline.return_value = mock_pipeline_instance
        mock_pipeline_instance.run.return_value = MagicMock()
        
        # Executa dry run
        result = run_pipeline(
            destination="duckdb",
            dataset_name="test_data",
            resource_type="sync"
        )
        
        # Verifica que pipeline foi criado e executado
        mock_dlt_pipeline.assert_called_once()
        mock_pipeline_instance.run.assert_called_once_with(mock_source)
        assert result is not None
    
    @pytest.mark.integration
    def test_pipeline_config_validation(self):
        """Teste de integração que valida a configuração real."""
        try:
            # Tenta criar o source com configuração real
            source = baliza_source(
                base_url="https://pncp.gov.br/api/consulta",
                resource_type="sync"
            )
            
            # Se chegou aqui, a configuração está válida
            assert source is not None
            
        except Exception as e:
            pytest.fail(f"Falha na validação da configuração: {e}")


class TestPipelineIntegration:
    """Testes de integração com a API real (marcados para execução opcional)."""
    
    @pytest.mark.integration
    @pytest.mark.slow  
    def test_api_connectivity(self):
        """Testa conectividade com a API PNCP real."""
        import requests
        
        base_url = "https://pncp.gov.br/api/consulta"
        
        # Testa endpoint de contratações (sem autenticação)
        response = requests.get(
            f"{base_url}/v1/contratacoes/publicacao",
            params={
                "dataInicial": "2024-01-01",
                "dataFinal": "2024-01-02",
                "pagina": 1,
                "tamanhoPagina": 10
            },
            timeout=30
        )
        
        # Verifica que API está respondendo
        assert response.status_code == 200, f"API retornou status {response.status_code}"
        
        # Verifica estrutura da resposta
        data = response.json()
        assert "data" in data, "Resposta não contém campo 'data'"
        assert "totalPaginas" in data, "Resposta não contém campo 'totalPaginas'"
        assert "numeroPagina" in data, "Resposta não contém campo 'numeroPagina'"
    
    @pytest.mark.integration
    @pytest.mark.slow
    def test_pipeline_full_dry_run(self):
        """Teste completo de dry run com API real."""
        pytest.importorskip("dlt")  # Pula se DLT não estiver instalado
        
        # Configura pipeline para dry run
        pipeline = dlt.pipeline(
            pipeline_name="baliza_test",
            destination="duckdb",
            dataset_name="test_data",
            dev_mode=True  # Modo desenvolvimento
        )
        
        # Cria source com configuração mínima
        source = baliza_source(
            base_url="https://pncp.gov.br/api/consulta",
            resource_type="sync"
        )
        
        # Executa dry run (não carrega dados, só testa a configuração)
        try:
            info = pipeline.run(source, table_name="test_table")
            assert info is not None
            print(f"Dry run executado com sucesso: {len(info.load_packages)} packages")
            
        except Exception as e:
            pytest.fail(f"Dry run falhou: {e}")


if __name__ == "__main__":
    # Executa testes básicos quando rodado diretamente
    pytest.main([__file__, "-v", "-x"])  # -x para parar no primeiro erro