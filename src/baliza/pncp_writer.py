"""PNCP Writer - DuckDB Single-Writer Architecture with Split Tables

Implements centralized write operations to DuckDB as specified in ADR-001 and ADR-008.
Uses split table architecture for content deduplication and optimized storage.
Ensures single-writer constraint through file locking and centralized write operations.
Manages raw data storage and task state tracking for the extraction pipeline.
"""

import asyncio
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
from filelock import FileLock, Timeout
from rich.console import Console

from .content_utils import analyze_content, is_empty_response
from .sql_loader import SQLLoader

logger = logging.getLogger(__name__)
console = Console(force_terminal=True, legacy_windows=False, stderr=False)

SQL_LOADER = SQLLoader()


# Data directory
DATA_DIR = Path.cwd() / "data"
BALIZA_DB_PATH = DATA_DIR / "baliza.duckdb"


def connect_utf8(path: str, force: bool = False) -> duckdb.DuckDBPyConnection:
    """Connect to DuckDB with UTF-8 error handling."""
    try:
        if force:
            # Force mode: try read-only connection first
            try:
                return duckdb.connect(path, read_only=True)
            except duckdb.Error:
                # If read-only fails, try regular connection
                pass

        return duckdb.connect(path)
    except duckdb.Error as exc:
        if force and "lock" in str(exc).lower():
            console.print(
                "⚠️ [yellow]Database locked by another process, trying read-only access[/yellow]"
            )
            try:
                return duckdb.connect(path, read_only=True)
            except duckdb.Error as read_exc:
                error_msg = (
                    f"Database connection failed even in read-only mode: {read_exc}"
                )
                raise RuntimeError(error_msg) from read_exc
        else:
            # DuckDB error - preserve original exception with clean message
            error_msg = f"Database connection failed: {exc}"
            raise RuntimeError(error_msg) from exc


class PNCPWriter:
    """Handles writing PNCP data to DuckDB."""

    def __init__(self, force_db: bool = False):
        self.conn: duckdb.DuckDBPyConnection | None = None
        self.db_lock: FileLock | None = None
        self.writer_running = False
        self.force_db = force_db
        self.db_path = str(BALIZA_DB_PATH)  # Add db_path property for TaskClaimer

    async def __aenter__(self):
        """Async context manager entry."""
        self._init_database()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with graceful cleanup."""
        # Note: exc_type, exc_val, exc_tb are part of the context manager protocol
        if self.conn:
            try:
                self.conn.commit()  # Commit any pending changes
                self.conn.close()
            except (duckdb.Error, AttributeError) as db_err:
                logger.warning(f"Error during database cleanup: {db_err}")

        if self.db_lock:
            try:
                self.db_lock.release()
            except (Timeout, RuntimeError) as lock_err:
                logger.warning(f"Error releasing database lock: {lock_err}")
        elif self.force_db:
            # In force mode, try to clean up any lock file we might have created
            try:
                lock_file = Path(str(BALIZA_DB_PATH) + ".lock")
                if lock_file.exists():
                    lock_file.unlink()
            except Exception as e:
                logger.warning(f"Could not clean up lock file in force mode: {e}")

    def _init_database(self):
        """Initialize DuckDB with PSA schema."""
        DATA_DIR.mkdir(parents=True, exist_ok=True)

        # Handle file locking based on force_db flag
        if self.force_db:
            # Force mode: Remove existing lock file if it exists
            lock_file = Path(str(BALIZA_DB_PATH) + ".lock")
            if lock_file.exists():
                lock_file.unlink()
                console.print(
                    "🔓 [yellow]Force mode: Removed existing database lock[/yellow]"
                )

            # Still create lock for this session but don't fail if can't acquire
            self.db_lock = FileLock(str(BALIZA_DB_PATH) + ".lock")
            try:
                self.db_lock.acquire(timeout=0.1)
            except Timeout:
                console.print(
                    "⚠️ [yellow]Warning: Could not acquire lock in force mode, proceeding anyway[/yellow]"
                )
                self.db_lock = None
        else:
            # Normal mode: Respect existing locks
            self.db_lock = FileLock(str(BALIZA_DB_PATH) + ".lock")
            try:
                self.db_lock.acquire(timeout=0.5)
            except Timeout:
                raise RuntimeError(
                    "Another instance is using the database. Use --force-db to override."
                )

        self.conn = connect_utf8(str(BALIZA_DB_PATH))

        # Create PSA schema
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS psa")

        # Create the split table architecture (ADR-008)

        # Table 1: Content storage with deduplication
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS psa.pncp_content (
                id UUID PRIMARY KEY, -- UUIDv5 based on content hash
                response_content TEXT NOT NULL,
                content_sha256 VARCHAR(64) NOT NULL UNIQUE, -- For integrity verification
                content_size_bytes INTEGER,
                first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                reference_count INTEGER DEFAULT 1 -- How many requests reference this content
            ) WITH (compression = "zstd")
        """
        )

        # Table 2: Request metadata with foreign key to content
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS psa.raw_pncp_requests (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                -- Identificação da Requisição
                endpoint_url VARCHAR NOT NULL,
                endpoint_name VARCHAR NOT NULL,
                
                -- Parâmetros da Requisição
                month VARCHAR(7), -- YYYY-MM format, NULL para endpoints sem range de datas
                request_parameters JSON, -- Todos os outros parâmetros
                
                -- Metadados da Resposta
                response_code INTEGER NOT NULL,
                response_headers JSON,
                total_records INTEGER, -- Total de registros disponíveis na API
                total_pages INTEGER,   -- Total de páginas disponíveis na API
                current_page INTEGER,  -- Página atual desta requisição
                page_size INTEGER,     -- Tamanho da página usado
                
                -- Controle de Execução
                run_id VARCHAR,        -- ID da execução/batch
                data_date DATE,        -- Data de referência dos dados
                
                -- Status de Processamento
                parse_status VARCHAR DEFAULT 'pending', -- pending, success, failed
                parse_error_message TEXT,
                records_parsed INTEGER DEFAULT 0, -- Quantos registros foram parseados com sucesso
                
                -- Timestamps
                parsed_at TIMESTAMP -- Quando o parsing foi concluído
            ) WITH (compression = "zstd")
        """
        )

        # Table 3: Parse errors for debugging
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS psa.pncp_parse_errors (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                
                -- Request Info
                endpoint_name VARCHAR NOT NULL,
                endpoint_url VARCHAR NOT NULL,
                http_status_code INTEGER,
                
                -- Response Data
                response_raw JSON, -- Raw response que falhou no parsing
                response_headers JSON,
                
                -- Error Info
                error_message TEXT NOT NULL,
                error_type VARCHAR, -- parse_error, validation_error, etc.
                stack_trace TEXT,
                
                -- Retry Control
                retry_count INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 3,
                next_retry_at TIMESTAMP,
                
                -- Timestamps
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) WITH (compression = "zstd")
        """
        )

        # Table 4: Bronze Contratos - Direct mapping from /v1/contratos endpoints
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS psa.raw_contratos (
                -- Identificadores
                numero_controle_pncp_compra VARCHAR,
                numero_controle_pncp VARCHAR,
                ano_contrato INTEGER,
                sequencial_contrato INTEGER,
                numero_contrato_empenho VARCHAR,
                
                -- Datas
                data_assinatura DATE,
                data_vigencia_inicio DATE,
                data_vigencia_fim DATE,
                data_publicacao_pncp TIMESTAMP,
                data_atualizacao TIMESTAMP,
                data_atualizacao_global TIMESTAMP,
                
                -- Fornecedor
                ni_fornecedor VARCHAR,
                tipo_pessoa VARCHAR(2), -- PJ, PF, PE
                nome_razao_social_fornecedor VARCHAR,
                codigo_pais_fornecedor VARCHAR,
                
                -- Subcontratação
                ni_fornecedor_subcontratado VARCHAR,
                nome_fornecedor_subcontratado VARCHAR,
                tipo_pessoa_subcontratada VARCHAR(2),
                
                -- Orgão/Entidade
                orgao_cnpj VARCHAR(14),
                orgao_razao_social VARCHAR,
                orgao_poder_id VARCHAR,
                orgao_esfera_id VARCHAR,
                
                -- Unidade
                unidade_codigo VARCHAR,
                unidade_nome VARCHAR,
                unidade_uf_sigla VARCHAR(2),
                unidade_uf_nome VARCHAR,
                unidade_municipio_nome VARCHAR,
                unidade_codigo_ibge VARCHAR,
                
                -- Subrogação
                orgao_subrogado_cnpj VARCHAR(14),
                orgao_subrogado_razao_social VARCHAR,
                unidade_subrogada_codigo VARCHAR,
                unidade_subrogada_nome VARCHAR,
                
                -- Tipo e Categoria
                tipo_contrato_id INTEGER,
                tipo_contrato_nome VARCHAR,
                categoria_processo_id INTEGER,
                categoria_processo_nome VARCHAR,
                
                -- Valores
                valor_inicial DECIMAL(15,4),
                valor_parcela DECIMAL(15,4),
                valor_global DECIMAL(15,4),
                valor_acumulado DECIMAL(15,4),
                
                -- Parcelas e Controle
                numero_parcelas INTEGER,
                numero_retificacao INTEGER,
                receita BOOLEAN,
                
                -- Texto e Observações
                objeto_contrato TEXT,
                informacao_complementar TEXT,
                processo VARCHAR,
                
                -- CIPI
                identificador_cipi VARCHAR,
                url_cipi VARCHAR,
                
                -- Metadados
                usuario_nome VARCHAR,
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                PRIMARY KEY (numero_controle_pncp, ano_contrato, sequencial_contrato)
            ) WITH (compression = "zstd")
        """
        )

        # Table 5: Bronze Contratacoes - Direct mapping from /v1/contratacoes/* endpoints
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS psa.raw_contratacoes (
                -- Identificadores
                numero_controle_pncp VARCHAR,
                ano_compra INTEGER,
                sequencial_compra INTEGER,
                numero_compra VARCHAR,
                processo VARCHAR,
                
                -- Datas
                data_inclusao TIMESTAMP,
                data_publicacao_pncp TIMESTAMP,
                data_atualizacao TIMESTAMP,
                data_atualizacao_global TIMESTAMP,
                data_abertura_proposta TIMESTAMP,
                data_encerramento_proposta TIMESTAMP,
                
                -- Orgão/Entidade
                orgao_cnpj VARCHAR(14),
                orgao_razao_social VARCHAR,
                orgao_poder_id VARCHAR,
                orgao_esfera_id VARCHAR,
                
                -- Unidade
                unidade_codigo VARCHAR,
                unidade_nome VARCHAR,
                unidade_uf_sigla VARCHAR(2),
                unidade_uf_nome VARCHAR,
                unidade_municipio_nome VARCHAR,
                unidade_codigo_ibge VARCHAR,
                
                -- Subrogação
                orgao_subrogado_cnpj VARCHAR(14),
                orgao_subrogado_razao_social VARCHAR,
                unidade_subrogada_codigo VARCHAR,
                unidade_subrogada_nome VARCHAR,
                
                -- Modalidade e Disputa
                modalidade_id INTEGER,
                modalidade_nome VARCHAR,
                modo_disputa_id INTEGER,
                modo_disputa_nome VARCHAR,
                
                -- Instrumento Convocatório
                tipo_instrumento_convocatorio_codigo INTEGER,
                tipo_instrumento_convocatorio_nome VARCHAR,
                
                -- Amparo Legal
                amparo_legal_codigo INTEGER,
                amparo_legal_nome VARCHAR,
                amparo_legal_descricao TEXT,
                
                -- Valores
                valor_total_estimado DECIMAL(15,4),
                valor_total_homologado DECIMAL(15,4),
                
                -- Situação
                situacao_compra_id VARCHAR, -- ENUM: 1,2,3,4
                situacao_compra_nome VARCHAR,
                
                -- Flags
                srp BOOLEAN, -- Sistema de Registro de Preços
                
                -- Texto
                objeto_compra TEXT,
                informacao_complementar TEXT,
                justificativa_presencial TEXT,
                
                -- Links
                link_sistema_origem VARCHAR,
                link_processo_eletronico VARCHAR,
                
                -- Metadados
                usuario_nome VARCHAR,
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                PRIMARY KEY (numero_controle_pncp)
            ) WITH (compression = "zstd")
        """
        )

        # Table 6: Bronze Atas - Direct mapping from /v1/atas/* endpoints
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS psa.raw_atas (
                -- Identificadores
                numero_controle_pncp_ata VARCHAR,
                numero_ata_registro_preco VARCHAR,
                ano_ata INTEGER,
                numero_controle_pncp_compra VARCHAR, -- FK para contratação
                
                -- Controle
                cancelado BOOLEAN,
                data_cancelamento TIMESTAMP,
                
                -- Datas
                data_assinatura TIMESTAMP,
                vigencia_inicio TIMESTAMP,
                vigencia_fim TIMESTAMP,
                data_publicacao_pncp TIMESTAMP,
                data_inclusao TIMESTAMP,
                data_atualizacao TIMESTAMP,
                data_atualizacao_global TIMESTAMP,
                
                -- Orgão Principal
                cnpj_orgao VARCHAR(14),
                nome_orgao VARCHAR,
                codigo_unidade_orgao VARCHAR,
                nome_unidade_orgao VARCHAR,
                
                -- Orgão Subrogado
                cnpj_orgao_subrogado VARCHAR(14),
                nome_orgao_subrogado VARCHAR,
                codigo_unidade_orgao_subrogado VARCHAR,
                nome_unidade_orgao_subrogado VARCHAR,
                
                -- Objeto
                objeto_contratacao TEXT,
                
                -- Metadados
                usuario VARCHAR,
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                PRIMARY KEY (numero_controle_pncp_ata)
            ) WITH (compression = "zstd")
        """
        )

        # Table 7: Bronze Fontes Orcamentarias - Child table of contratacoes
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS psa.raw_fontes_orcamentarias (
                -- Relacionamento
                contratacao_numero_controle_pncp VARCHAR NOT NULL, -- FK para raw_contratacoes
                
                -- Dados da Fonte
                codigo INTEGER,
                nome VARCHAR,
                descricao TEXT,
                data_inclusao TIMESTAMP,
                
                -- Metadados
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                PRIMARY KEY (contratacao_numero_controle_pncp, codigo)
            ) WITH (compression = "zstd")
        """
        )

        # Legacy table for backwards compatibility during migration
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS psa.pncp_raw_responses (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                endpoint_url VARCHAR NOT NULL,
                endpoint_name VARCHAR NOT NULL,
                request_parameters JSON,
                response_code INTEGER NOT NULL,
                response_content TEXT,
                response_headers JSON,
                data_date DATE,
                run_id VARCHAR,
                total_records INTEGER,
                total_pages INTEGER,
                current_page INTEGER,
                page_size INTEGER
            ) WITH (compression = "zstd")
        """
        )

        # Create indexes if they don't exist
        self._create_indexes_if_not_exist()

    def _index_exists(self, index_name: str) -> bool:
        """Check if a given index exists in the database."""
        try:
            # Query information_schema to check if index exists
            result = self.conn.execute(
                "SELECT 1 FROM information_schema.indexes WHERE index_name = ?",
                [index_name],
            ).fetchone()
            return result is not None
        except duckdb.Error:
            # Fallback: try to create the index and catch the error
            try:
                self.conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {index_name}_test ON psa.pncp_raw_responses(endpoint_name)"
                )
                self.conn.execute(f"DROP INDEX IF EXISTS {index_name}_test")
                return False  # If we can create a test index, the target doesn't exist
            except duckdb.Error:
                return True  # If we can't create test index, assume target exists

    def _create_indexes_if_not_exist(self):
        """Create indexes for split table architecture."""
        indexes_to_create = {
            # Indexes for new split tables
            # Indexes for raw_pncp_requests
            "idx_requests_endpoint_month": "CREATE INDEX IF NOT EXISTS idx_requests_endpoint_month ON psa.raw_pncp_requests(endpoint_name, month)",
            "idx_requests_status": "CREATE INDEX IF NOT EXISTS idx_requests_status ON psa.raw_pncp_requests(parse_status)",
            "idx_requests_date": "CREATE INDEX IF NOT EXISTS idx_requests_date ON psa.raw_pncp_requests(extracted_at)",
            "idx_requests_run_id": "CREATE INDEX IF NOT EXISTS idx_requests_run_id ON psa.raw_pncp_requests(run_id)",
            
            # Indexes for raw_contratos
            "idx_contratos_pncp": "CREATE INDEX IF NOT EXISTS idx_contratos_pncp ON psa.raw_contratos(numero_controle_pncp)",
            "idx_contratos_orgao": "CREATE INDEX IF NOT EXISTS idx_contratos_orgao ON psa.raw_contratos(orgao_cnpj)",
            "idx_contratos_fornecedor": "CREATE INDEX IF NOT EXISTS idx_contratos_fornecedor ON psa.raw_contratos(ni_fornecedor)",
            "idx_contratos_data": "CREATE INDEX IF NOT EXISTS idx_contratos_data ON psa.raw_contratos(data_publicacao_pncp)",
            
            # Indexes for raw_contratacoes
            "idx_contratacoes_pncp": "CREATE INDEX IF NOT EXISTS idx_contratacoes_pncp ON psa.raw_contratacoes(numero_controle_pncp)",
            "idx_contratacoes_orgao": "CREATE INDEX IF NOT EXISTS idx_contratacoes_orgao ON psa.raw_contratacoes(orgao_cnpj)",
            "idx_contratacoes_modalidade": "CREATE INDEX IF NOT EXISTS idx_contratacoes_modalidade ON psa.raw_contratacoes(modalidade_id)",
            "idx_contratacoes_data": "CREATE INDEX IF NOT EXISTS idx_contratacoes_data ON psa.raw_contratacoes(data_publicacao_pncp)",
            
            # Indexes for raw_atas
            "idx_atas_pncp": "CREATE INDEX IF NOT EXISTS idx_atas_pncp ON psa.raw_atas(numero_controle_pncp_ata)",
            "idx_atas_compra": "CREATE INDEX IF NOT EXISTS idx_atas_compra ON psa.raw_atas(numero_controle_pncp_compra)",
            "idx_atas_orgao": "CREATE INDEX IF NOT EXISTS idx_atas_orgao ON psa.raw_atas(cnpj_orgao)",
            
            # Indexes for raw_fontes_orcamentarias
            "idx_fontes_contratacao": "CREATE INDEX IF NOT EXISTS idx_fontes_contratacao ON psa.raw_fontes_orcamentarias(contratacao_numero_controle_pncp)",
            
            # Indexes for pncp_parse_errors
            "idx_parse_errors_endpoint": "CREATE INDEX IF NOT EXISTS idx_parse_errors_endpoint ON psa.pncp_parse_errors(endpoint_name)",
            "idx_parse_errors_retry": "CREATE INDEX IF NOT EXISTS idx_parse_errors_retry ON psa.pncp_parse_errors(retry_count, next_retry_at)",
            "idx_content_hash": "CREATE INDEX IF NOT EXISTS idx_content_hash ON psa.pncp_content(content_sha256)",
            "idx_content_first_seen": "CREATE INDEX IF NOT EXISTS idx_content_first_seen ON psa.pncp_content(first_seen_at)",
            # Legacy table indexes for backwards compatibility
            "idx_legacy_endpoint_date_page": "CREATE INDEX IF NOT EXISTS idx_legacy_endpoint_date_page ON psa.pncp_raw_responses(endpoint_name, data_date, current_page)",
            "idx_legacy_response_code": "CREATE INDEX IF NOT EXISTS idx_legacy_response_code ON psa.pncp_raw_responses(response_code)",
        }

        for idx_name, create_sql in indexes_to_create.items():
            try:
                self.conn.execute(create_sql)
                logger.info(f"Index '{idx_name}' ensured.")
            except duckdb.Error as e:
                logger.exception(f"Failed to create index '{idx_name}': {e}")

    def _batch_store_responses(self, responses: list[dict[str, Any]]):
        """Store multiple responses in a single batch operation with transaction."""
        if not responses:
            return

        # Prepare batch data
        batch_data = []
        for resp in responses:
            batch_data.append(
                [
                    resp["endpoint_url"],
                    resp["endpoint_name"],
                    json.dumps(resp["request_parameters"]),
                    resp["response_code"],
                    resp["response_content"],
                    json.dumps(resp["response_headers"]),
                    resp["data_date"],
                    resp["run_id"],
                    resp["total_records"],
                    resp["total_pages"],
                    resp["current_page"],
                    resp["page_size"],
                ]
            )

        # Batch insert with transaction
        self.conn.execute("BEGIN TRANSACTION")
        try:
            insert_sql = SQL_LOADER.load("dml/inserts/pncp_raw_responses.sql")
            self.conn.executemany(
                insert_sql,
                batch_data,
            )
            self.conn.execute("COMMIT")
        except duckdb.Error as e:
            self.conn.execute("ROLLBACK")
            logger.exception(f"Batch store failed: {e}")
            raise

    def _ensure_content_exists(self, content: str) -> str:
        """Ensure content exists in psa.pncp_content table and return content_id.

        Uses content deduplication logic from ADR-008.

        Args:
            content: Response content string

        Returns:
            UUID string of the content record
        """
        if is_empty_response(content):
            # For empty responses, create a special empty content record
            content = ""

        # Analyze content to get ID and hash
        content_id, content_hash, content_size = analyze_content(content)

        try:
            # Try to find existing content by hash
            existing = self.conn.execute(
                "SELECT id, reference_count FROM psa.pncp_content WHERE content_sha256 = ?",
                [content_hash],
            ).fetchone()

            if existing:
                # Content exists - increment reference count and update last_seen_at
                self.conn.execute(
                    """
                    UPDATE psa.pncp_content
                    SET reference_count = reference_count + 1,
                        last_seen_at = CURRENT_TIMESTAMP
                    WHERE content_sha256 = ?
                    """,
                    [content_hash],
                )
                logger.debug(
                    f"Content deduplicated: {content_id} (new ref count: {existing[1] + 1})"
                )
                return existing[0]  # Return existing content_id
            else:
                # New content - insert new record
                insert_sql = SQL_LOADER.load("dml/inserts/pncp_content.sql")
                self.conn.execute(
                    insert_sql,
                    [content_id, content, content_hash, content_size],
                )
                logger.debug(f"New content stored: {content_id} ({content_size} bytes)")
                return content_id

        except duckdb.Error as e:
            logger.exception(f"Content storage failed for hash {content_hash}: {e}")
            raise

    def _store_request_with_content_id(self, page_data: dict, content_id: str) -> None:
        """Store request metadata with reference to deduplicated content."""
        try:
            insert_sql = SQL_LOADER.load("dml/inserts/pncp_requests.sql")
            self.conn.execute(
                insert_sql,
                [
                    page_data.get("extracted_at"),
                    page_data.get("endpoint_url"),
                    page_data.get("endpoint_name"),
                    json.dumps(page_data.get("request_parameters", {})),
                    page_data.get("response_code"),
                    json.dumps(page_data.get("response_headers", {})),
                    page_data.get("data_date"),
                    page_data.get("run_id"),
                    page_data.get("total_records"),
                    page_data.get("total_pages"),
                    page_data.get("current_page"),
                    page_data.get("page_size"),
                    content_id,
                ],
            )
        except duckdb.Error as e:
            logger.exception(f"Request storage failed: {e}")
            raise

    def _batch_store_split_tables(self, pages: list[dict]) -> None:
        """Store pages using split table architecture with optimized batch content deduplication."""
        if not pages:
            return

        # Step 1: Analyze all content in the batch and check for existing hashes
        content_map = {}  # content_hash -> (content_id, content, content_size)
        batch_cache = {}  # For within-batch deduplication

        for page_data in pages:
            content = page_data.get("response_content", "")
            if is_empty_response(content):
                content = ""

            # Check if we've already seen this content in this batch
            if content in batch_cache:
                page_data["_content_id"] = batch_cache[content]
                continue

            content_id, content_hash, content_size = analyze_content(content)
            content_map[content_hash] = (content_id, content, content_size)
            batch_cache[content] = content_id
            page_data["_content_id"] = content_id
            page_data["_content_hash"] = content_hash

        # Step 2: Batch check for existing content hashes
        if content_map:
            placeholders = ",".join("?" * len(content_map))
            existing_content = self.conn.execute(
                f"SELECT content_sha256, id, reference_count FROM psa.pncp_content WHERE content_sha256 IN ({placeholders})",
                list(content_map.keys()),
            ).fetchall()

            existing_hashes = {
                content_hash: (content_id, ref_count)
                for content_hash, content_id, ref_count in existing_content
            }

            # Step 3: Batch insert new content and update reference counts
            new_content_records = []
            update_ref_counts = []

            for content_hash, (
                content_id,
                content,
                content_size,
            ) in content_map.items():
                if content_hash in existing_hashes:
                    # Content exists - mark for reference count update
                    existing_id, ref_count = existing_hashes[content_hash]
                    update_ref_counts.append((content_hash, ref_count + 1))
                    # Update our mapping to use existing content_id
                    for page_data in pages:
                        if page_data.get("_content_hash") == content_hash:
                            page_data["_content_id"] = existing_id
                else:
                    # New content - mark for insertion
                    new_content_records.append(
                        (content_id, content, content_hash, content_size)
                    )

            # Batch insert new content
            if new_content_records:
                insert_sql = SQL_LOADER.load("dml/inserts/pncp_content.sql")
                self.conn.executemany(
                    insert_sql,
                    new_content_records,
                )
                logger.debug(
                    f"Batch inserted {len(new_content_records)} new content records"
                )

            # Batch update reference counts
            if update_ref_counts:
                self.conn.executemany(
                    """
                    UPDATE psa.pncp_content 
                    SET reference_count = ?, last_seen_at = CURRENT_TIMESTAMP
                    WHERE content_sha256 = ?
                    """,
                    [
                        (new_count, content_hash)
                        for content_hash, new_count in update_ref_counts
                    ],
                )
                logger.debug(
                    f"Batch updated {len(update_ref_counts)} content reference counts"
                )

        # Step 4: Batch store requests using the resolved content_ids
        request_records = []
        for page_data in pages:
            content_id = page_data["_content_id"]
            request_records.append(
                [
                    page_data.get("extracted_at"),
                    page_data.get("endpoint_url"),
                    page_data.get("endpoint_name"),
                    json.dumps(page_data.get("request_parameters", {})),
                    page_data.get("response_code"),
                    json.dumps(page_data.get("response_headers", {})),
                    page_data.get("data_date"),
                    page_data.get("run_id"),
                    page_data.get("total_records"),
                    page_data.get("total_pages"),
                    page_data.get("current_page"),
                    page_data.get("page_size"),
                    content_id,
                ]
            )

        if request_records:
            insert_sql = SQL_LOADER.load("dml/inserts/pncp_requests.sql")
            self.conn.executemany(
                insert_sql,
                request_records,
            )

        # Step 5: Also store in legacy table for backwards compatibility
        for page_data in pages:
            self._store_legacy_response(page_data)

    def _store_legacy_response(self, page_data: dict) -> None:
        """Store in legacy pncp_raw_responses table for backwards compatibility."""
        try:
            insert_sql = SQL_LOADER.load("dml/inserts/pncp_raw_responses.sql")
            self.conn.execute(
                insert_sql,
                [
                    page_data.get("extracted_at"),
                    page_data.get("endpoint_url"),
                    page_data.get("endpoint_name"),
                    json.dumps(page_data.get("request_parameters", {})),
                    page_data.get("response_code"),
                    page_data.get("response_content", ""),
                    json.dumps(page_data.get("response_headers", {})),
                    page_data.get("data_date"),
                    page_data.get("run_id"),
                    page_data.get("total_records"),
                    page_data.get("total_pages"),
                    page_data.get("current_page"),
                    page_data.get("page_size"),
                ],
            )
        except duckdb.Error as e:
            logger.exception(f"Legacy response storage failed: {e}")
            raise

    async def writer_worker(
        self, page_queue: asyncio.Queue, commit_every: int = 75
    ) -> None:
        """Dedicated writer coroutine for single-threaded DB writes.

        Optimized for:
        - commit_every=75 pages ≈ 5-8 seconds between commits
        - Reduces I/O overhead by 7.5x (10→75 pages per commit)
        - Local batch buffer minimizes executemany() calls
        """
        counter = 0
        batch_buffer = []

        while True:
            try:
                # Get page from queue (None is sentinel to stop)
                page = await page_queue.get()
                if page is None:
                    break

                # Add to local buffer
                batch_buffer.append(page)
                counter += 1

                # Flush buffer every commit_every pages
                if counter % commit_every == 0 and batch_buffer:
                    self._batch_store_split_tables(
                        batch_buffer
                    )  # Use new split table logic
                    self.conn.commit()
                    batch_buffer.clear()

                # Mark task as done
                page_queue.task_done()

            except duckdb.Error as e:
                console.print(f"❌ Writer error: {e}")
                page_queue.task_done()
                break

        # Flush any remaining items
        if batch_buffer:
            self._batch_store_split_tables(batch_buffer)  # Use new split table logic
            self.conn.commit()

        self.writer_running = False

    def get_raw_content(self, endpoint_name: str, data_date: date, page: int) -> str:
        """Retrieve raw JSON content from database."""
        result = self.conn.execute(
            """
            SELECT response_content FROM psa.pncp_raw_responses
            WHERE endpoint_name = ? AND data_date = ? AND current_page = ? AND response_code = 200
            LIMIT 1
        """,
            [endpoint_name, data_date, page],
        ).fetchone()

        if not result:
            raise ValueError(f"Page not found: {endpoint_name}, {data_date}, {page}")

        return result[0]

    def __del__(self):
        """Cleanup."""
        if hasattr(self, "conn"):
            self.conn.close()
