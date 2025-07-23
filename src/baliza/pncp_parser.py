"""Direct-to-table parser for PNCP API responses."""

import json
import traceback
from datetime import datetime
from typing import List, Optional, Tuple, Dict, Any
from uuid import uuid4

from pydantic import ValidationError

from baliza.pncp_schemas import (
    BronzeContrato,
    BronzeContratacao,
    BronzeFonteOrcamentaria,
    BronzeAta,
    BronzeRequest,
    ParseError
)


class PNCPDirectParser:
    """Direct-to-table parser for PNCP API responses.
    
    Eliminates raw content persistence, parsing directly to structured tables.
    Only stores parse errors for debugging.
    """
    
    def __init__(self, conn):
        """Initialize parser with database connection."""
        self.conn = conn
    
    def parse_response(
        self, 
        endpoint_name: str, 
        endpoint_url: str,
        response_data: dict,
        month: Optional[str] = None,
        request_parameters: Optional[dict] = None,
        run_id: Optional[str] = None
    ) -> Tuple[int, List[str]]:
        """Parse API response directly to bronze tables.
        
        Args:
            endpoint_name: Name of the endpoint (contratos_publicacao, etc.)
            endpoint_url: Full URL of the request
            response_data: Raw response data from API
            month: Month in YYYY-MM format (None for endpoints without date range)
            request_parameters: Additional request parameters
            run_id: Execution batch ID
            
        Returns:
            Tuple of (records_parsed, errors)
        """
        records_parsed = 0
        errors = []
        
        # Extract pagination metadata
        total_records = response_data.get("totalRegistros", 0)
        total_pages = response_data.get("totalPaginas", 0)
        current_page = response_data.get("numeroPagina", 1)
        page_size = len(response_data.get("data", []))
        
        # Create request metadata record
        request_record = BronzeRequest(
            endpoint_name=endpoint_name,
            endpoint_url=endpoint_url,
            month=month,
            request_parameters=request_parameters or {},
            response_code=200,  # Assuming success if we got here
            total_records=total_records,
            total_pages=total_pages,
            current_page=current_page,
            page_size=page_size,
            run_id=run_id,
            data_date=datetime.now().date(),
            parse_status="pending"
        )
        
        try:
            # Route to specific parser based on endpoint
            if endpoint_name in ["contratos_publicacao", "contratos_atualizacao"]:
                records_parsed, errors = self._parse_contratos(response_data.get("data", []))
            elif endpoint_name in ["contratacoes_publicacao", "contratacoes_atualizacao", "contratacoes_proposta"]:
                records_parsed, errors = self._parse_contratacoes(response_data.get("data", []))
            elif endpoint_name in ["atas_periodo", "atas_atualizacao"]:
                records_parsed, errors = self._parse_atas(response_data.get("data", []))
            else:
                error_msg = f"Unknown endpoint: {endpoint_name}"
                errors.append(error_msg)
                self._log_parse_error(endpoint_name, endpoint_url, response_data, error_msg, "unknown_endpoint")
            
            # Update request record with results
            request_record.records_parsed = records_parsed
            request_record.parse_status = "success" if not errors else "failed"
            request_record.parse_error_message = "; ".join(errors) if errors else None
            request_record.parsed_at = datetime.now()
            
        except Exception as e:
            error_msg = f"Critical parsing error: {str(e)}"
            errors.append(error_msg)
            request_record.parse_status = "failed"
            request_record.parse_error_message = error_msg
            
            # Log the critical error with full context
            self._log_parse_error(
                endpoint_name, 
                endpoint_url, 
                response_data, 
                error_msg, 
                "critical_error",
                traceback.format_exc()
            )
        
        # Save request metadata
        self._save_request_metadata(request_record)
        
        return records_parsed, errors
    
    def _parse_contratos(self, contratos_data: List[dict]) -> Tuple[int, List[str]]:
        """Parse contratos from /v1/contratos endpoints."""
        records_parsed = 0
        errors = []
        
        for contrato_data in contratos_data:
            try:
                # Parse to Pydantic model for validation
                bronze_contrato = BronzeContrato.from_api_response(contrato_data)
                
                # Insert into bronze_contratos table
                self._insert_bronze_contrato(bronze_contrato)
                records_parsed += 1
                
            except ValidationError as e:
                error_msg = f"Validation error for contrato {contrato_data.get('numeroControlePNCP', 'unknown')}: {str(e)}"
                errors.append(error_msg)
                self._log_parse_error(
                    "contratos_parsing", 
                    "", 
                    contrato_data, 
                    error_msg, 
                    "validation_error"
                )
            except Exception as e:
                error_msg = f"Parse error for contrato {contrato_data.get('numeroControlePNCP', 'unknown')}: {str(e)}"
                errors.append(error_msg)
                self._log_parse_error(
                    "contratos_parsing", 
                    "", 
                    contrato_data, 
                    error_msg, 
                    "parse_error"
                )
        
        return records_parsed, errors
    
    def _parse_contratacoes(self, contratacoes_data: List[dict]) -> Tuple[int, List[str]]:
        """Parse contratacoes from /v1/contratacoes/* endpoints."""
        records_parsed = 0
        errors = []
        
        for contratacao_data in contratacoes_data:
            try:
                # Parse main contratacao record
                bronze_contratacao = BronzeContratacao.from_api_response(contratacao_data)
                self._insert_bronze_contratacao(bronze_contratacao)
                records_parsed += 1
                
                # Parse fontes orçamentárias (child records)
                fontes_orcamentarias = contratacao_data.get("fontesOrcamentarias", [])
                for fonte_data in fontes_orcamentarias:
                    try:
                        bronze_fonte = BronzeFonteOrcamentaria.from_api_response(
                            fonte_data, 
                            bronze_contratacao.numero_controle_pncp
                        )
                        self._insert_bronze_fonte_orcamentaria(bronze_fonte)
                    except Exception as e:
                        error_msg = f"Parse error for fonte orçamentária: {str(e)}"
                        errors.append(error_msg)
                
            except ValidationError as e:
                error_msg = f"Validation error for contratacao {contratacao_data.get('numeroControlePNCP', 'unknown')}: {str(e)}"
                errors.append(error_msg)
                self._log_parse_error(
                    "contratacoes_parsing", 
                    "", 
                    contratacao_data, 
                    error_msg, 
                    "validation_error"
                )
            except Exception as e:
                error_msg = f"Parse error for contratacao {contratacao_data.get('numeroControlePNCP', 'unknown')}: {str(e)}"
                errors.append(error_msg)
                self._log_parse_error(
                    "contratacoes_parsing", 
                    "", 
                    contratacao_data, 
                    error_msg, 
                    "parse_error"
                )
        
        return records_parsed, errors
    
    def _parse_atas(self, atas_data: List[dict]) -> Tuple[int, List[str]]:
        """Parse atas from /v1/atas/* endpoints."""
        records_parsed = 0
        errors = []
        
        for ata_data in atas_data:
            try:
                # Parse to Pydantic model for validation
                bronze_ata = BronzeAta.from_api_response(ata_data)
                
                # Insert into bronze_atas table
                self._insert_bronze_ata(bronze_ata)
                records_parsed += 1
                
            except ValidationError as e:
                error_msg = f"Validation error for ata {ata_data.get('numeroControlePNCPAta', 'unknown')}: {str(e)}"
                errors.append(error_msg)
                self._log_parse_error(
                    "atas_parsing", 
                    "", 
                    ata_data, 
                    error_msg, 
                    "validation_error"
                )
            except Exception as e:
                error_msg = f"Parse error for ata {ata_data.get('numeroControlePNCPAta', 'unknown')}: {str(e)}"
                errors.append(error_msg)
                self._log_parse_error(
                    "atas_parsing", 
                    "", 
                    ata_data, 
                    error_msg, 
                    "parse_error"
                )
        
        return records_parsed, errors
    
    def _insert_bronze_contrato(self, contrato: BronzeContrato):
        """Insert BronzeContrato into bronze_contratos table."""
        insert_sql = """
        INSERT OR REPLACE INTO psa.bronze_contratos (
            numero_controle_pncp_compra, numero_controle_pncp, ano_contrato, sequencial_contrato,
            numero_contrato_empenho, data_assinatura, data_vigencia_inicio, data_vigencia_fim,
            data_publicacao_pncp, data_atualizacao, data_atualizacao_global,
            ni_fornecedor, tipo_pessoa, nome_razao_social_fornecedor, codigo_pais_fornecedor,
            ni_fornecedor_subcontratado, nome_fornecedor_subcontratado, tipo_pessoa_subcontratada,
            orgao_cnpj, orgao_razao_social, orgao_poder_id, orgao_esfera_id,
            unidade_codigo, unidade_nome, unidade_uf_sigla, unidade_uf_nome, 
            unidade_municipio_nome, unidade_codigo_ibge,
            orgao_subrogado_cnpj, orgao_subrogado_razao_social, unidade_subrogada_codigo, unidade_subrogada_nome,
            tipo_contrato_id, tipo_contrato_nome, categoria_processo_id, categoria_processo_nome,
            valor_inicial, valor_parcela, valor_global, valor_acumulado,
            numero_parcelas, numero_retificacao, receita,
            objeto_contrato, informacao_complementar, processo,
            identificador_cipi, url_cipi, usuario_nome, extracted_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.conn.execute(insert_sql, (
            contrato.numero_controle_pncp_compra, contrato.numero_controle_pncp, 
            contrato.ano_contrato, contrato.sequencial_contrato,
            contrato.numero_contrato_empenho, contrato.data_assinatura, 
            contrato.data_vigencia_inicio, contrato.data_vigencia_fim,
            contrato.data_publicacao_pncp, contrato.data_atualizacao, contrato.data_atualizacao_global,
            contrato.ni_fornecedor, contrato.tipo_pessoa, contrato.nome_razao_social_fornecedor, 
            contrato.codigo_pais_fornecedor, contrato.ni_fornecedor_subcontratado, 
            contrato.nome_fornecedor_subcontratado, contrato.tipo_pessoa_subcontratada,
            contrato.orgao_cnpj, contrato.orgao_razao_social, contrato.orgao_poder_id, contrato.orgao_esfera_id,
            contrato.unidade_codigo, contrato.unidade_nome, contrato.unidade_uf_sigla, 
            contrato.unidade_uf_nome, contrato.unidade_municipio_nome, contrato.unidade_codigo_ibge,
            contrato.orgao_subrogado_cnpj, contrato.orgao_subrogado_razao_social, 
            contrato.unidade_subrogada_codigo, contrato.unidade_subrogada_nome,
            contrato.tipo_contrato_id, contrato.tipo_contrato_nome, 
            contrato.categoria_processo_id, contrato.categoria_processo_nome,
            contrato.valor_inicial, contrato.valor_parcela, contrato.valor_global, contrato.valor_acumulado,
            contrato.numero_parcelas, contrato.numero_retificacao, contrato.receita,
            contrato.objeto_contrato, contrato.informacao_complementar, contrato.processo,
            contrato.identificador_cipi, contrato.url_cipi, contrato.usuario_nome, contrato.extracted_at
        ))
    
    def _insert_bronze_contratacao(self, contratacao: BronzeContratacao):
        """Insert BronzeContratacao into bronze_contratacoes table."""
        insert_sql = """
        INSERT OR REPLACE INTO psa.bronze_contratacoes (
            numero_controle_pncp, ano_compra, sequencial_compra, numero_compra, processo,
            data_inclusao, data_publicacao_pncp, data_atualizacao, data_atualizacao_global,
            data_abertura_proposta, data_encerramento_proposta,
            orgao_cnpj, orgao_razao_social, orgao_poder_id, orgao_esfera_id,
            unidade_codigo, unidade_nome, unidade_uf_sigla, unidade_uf_nome,
            unidade_municipio_nome, unidade_codigo_ibge,
            orgao_subrogado_cnpj, orgao_subrogado_razao_social, unidade_subrogada_codigo, unidade_subrogada_nome,
            modalidade_id, modalidade_nome, modo_disputa_id, modo_disputa_nome,
            tipo_instrumento_convocatorio_codigo, tipo_instrumento_convocatorio_nome,
            amparo_legal_codigo, amparo_legal_nome, amparo_legal_descricao,
            valor_total_estimado, valor_total_homologado,
            situacao_compra_id, situacao_compra_nome, srp,
            objeto_compra, informacao_complementar, justificativa_presencial,
            link_sistema_origem, link_processo_eletronico,
            usuario_nome, extracted_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.conn.execute(insert_sql, (
            contratacao.numero_controle_pncp, contratacao.ano_compra, contratacao.sequencial_compra,
            contratacao.numero_compra, contratacao.processo,
            contratacao.data_inclusao, contratacao.data_publicacao_pncp, contratacao.data_atualizacao,
            contratacao.data_atualizacao_global, contratacao.data_abertura_proposta, contratacao.data_encerramento_proposta,
            contratacao.orgao_cnpj, contratacao.orgao_razao_social, contratacao.orgao_poder_id, contratacao.orgao_esfera_id,
            contratacao.unidade_codigo, contratacao.unidade_nome, contratacao.unidade_uf_sigla, contratacao.unidade_uf_nome,
            contratacao.unidade_municipio_nome, contratacao.unidade_codigo_ibge,
            contratacao.orgao_subrogado_cnpj, contratacao.orgao_subrogado_razao_social, 
            contratacao.unidade_subrogada_codigo, contratacao.unidade_subrogada_nome,
            contratacao.modalidade_id, contratacao.modalidade_nome, contratacao.modo_disputa_id, contratacao.modo_disputa_nome,
            contratacao.tipo_instrumento_convocatorio_codigo, contratacao.tipo_instrumento_convocatorio_nome,
            contratacao.amparo_legal_codigo, contratacao.amparo_legal_nome, contratacao.amparo_legal_descricao,
            contratacao.valor_total_estimado, contratacao.valor_total_homologado,
            contratacao.situacao_compra_id, contratacao.situacao_compra_nome, contratacao.srp,
            contratacao.objeto_compra, contratacao.informacao_complementar, contratacao.justificativa_presencial,
            contratacao.link_sistema_origem, contratacao.link_processo_eletronico,
            contratacao.usuario_nome, contratacao.extracted_at
        ))
    
    def _insert_bronze_fonte_orcamentaria(self, fonte: BronzeFonteOrcamentaria):
        """Insert BronzeFonteOrcamentaria into bronze_fontes_orcamentarias table."""
        insert_sql = """
        INSERT OR REPLACE INTO psa.bronze_fontes_orcamentarias (
            contratacao_numero_controle_pncp, codigo, nome, descricao, data_inclusao, extracted_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """
        
        self.conn.execute(insert_sql, (
            fonte.contratacao_numero_controle_pncp, fonte.codigo, fonte.nome,
            fonte.descricao, fonte.data_inclusao, fonte.extracted_at
        ))
    
    def _insert_bronze_ata(self, ata: BronzeAta):
        """Insert BronzeAta into bronze_atas table."""
        insert_sql = """
        INSERT OR REPLACE INTO psa.bronze_atas (
            numero_controle_pncp_ata, numero_ata_registro_preco, ano_ata, numero_controle_pncp_compra,
            cancelado, data_cancelamento,
            data_assinatura, vigencia_inicio, vigencia_fim, data_publicacao_pncp,
            data_inclusao, data_atualizacao, data_atualizacao_global,
            cnpj_orgao, nome_orgao, codigo_unidade_orgao, nome_unidade_orgao,
            cnpj_orgao_subrogado, nome_orgao_subrogado, codigo_unidade_orgao_subrogado, nome_unidade_orgao_subrogado,
            objeto_contratacao, usuario, extracted_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.conn.execute(insert_sql, (
            ata.numero_controle_pncp_ata, ata.numero_ata_registro_preco, ata.ano_ata, ata.numero_controle_pncp_compra,
            ata.cancelado, ata.data_cancelamento,
            ata.data_assinatura, ata.vigencia_inicio, ata.vigencia_fim, ata.data_publicacao_pncp,
            ata.data_inclusao, ata.data_atualizacao, ata.data_atualizacao_global,
            ata.cnpj_orgao, ata.nome_orgao, ata.codigo_unidade_orgao, ata.nome_unidade_orgao,
            ata.cnpj_orgao_subrogado, ata.nome_orgao_subrogado, ata.codigo_unidade_orgao_subrogado, ata.nome_unidade_orgao_subrogado,
            ata.objeto_contratacao, ata.usuario, ata.extracted_at
        ))
    
    def _save_request_metadata(self, request: BronzeRequest):
        """Save request metadata to bronze_pncp_requests table."""
        insert_sql = """
        INSERT INTO psa.bronze_pncp_requests (
            endpoint_name, endpoint_url, month, request_parameters,
            response_code, response_headers, total_records, total_pages,
            current_page, page_size, run_id, data_date,
            parse_status, parse_error_message, records_parsed,
            extracted_at, parsed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.conn.execute(insert_sql, (
            request.endpoint_name, request.endpoint_url, request.month,
            json.dumps(request.request_parameters) if request.request_parameters else None,
            request.response_code, 
            json.dumps(request.response_headers) if request.response_headers else None,
            request.total_records, request.total_pages, request.current_page, request.page_size,
            request.run_id, request.data_date, request.parse_status, request.parse_error_message,
            request.records_parsed, request.extracted_at, request.parsed_at
        ))
    
    def _log_parse_error(
        self, 
        endpoint_name: str, 
        endpoint_url: str, 
        response_data: dict, 
        error_message: str, 
        error_type: str, 
        stack_trace: Optional[str] = None
    ):
        """Log parse error to pncp_parse_errors table."""
        error_record = ParseError(
            endpoint_name=endpoint_name,
            endpoint_url=endpoint_url,
            response_raw=response_data,
            error_message=error_message,
            error_type=error_type,
            stack_trace=stack_trace
        )
        
        insert_sql = """
        INSERT INTO psa.pncp_parse_errors (
            endpoint_name, endpoint_url, http_status_code,
            response_raw, response_headers, error_message, error_type, stack_trace,
            retry_count, max_retries, next_retry_at,
            extracted_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.conn.execute(insert_sql, (
            error_record.endpoint_name, error_record.endpoint_url, error_record.http_status_code,
            json.dumps(error_record.response_raw) if error_record.response_raw else None,
            json.dumps(error_record.response_headers) if error_record.response_headers else None,
            error_record.error_message, error_record.error_type, error_record.stack_trace,
            error_record.retry_count, error_record.max_retries, error_record.next_retry_at,
            error_record.extracted_at, error_record.created_at, error_record.updated_at
        ))
    
    def check_already_processed(self, endpoint_name: str, month: Optional[str], request_parameters: Optional[dict] = None) -> bool:
        """Check if endpoint/month/parameters combination was already successfully processed."""
        check_sql = """
        SELECT 1 FROM psa.bronze_pncp_requests 
        WHERE endpoint_name = ? 
        AND month IS NOT DISTINCT FROM ?
        AND json(request_parameters) IS NOT DISTINCT FROM json(?)
        AND parse_status = 'success'
        LIMIT 1
        """
        
        result = self.conn.execute(check_sql, (
            endpoint_name, 
            month,
            json.dumps(request_parameters) if request_parameters else None
        )).fetchone()
        
        return result is not None
    
    def get_failed_requests_for_retry(self) -> List[Dict[str, Any]]:
        """Get failed requests that are eligible for retry."""
        retry_sql = """
        SELECT 
            endpoint_name, endpoint_url, month, request_parameters,
            parse_error_message, retry_count
        FROM psa.bronze_pncp_requests 
        WHERE parse_status = 'failed'
        AND retry_count < 3
        AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP)
        ORDER BY extracted_at DESC
        LIMIT 100
        """
        
        results = self.conn.execute(retry_sql).fetchall()
        return [dict(row) for row in results]