#!/usr/bin/env python3
"""
Generate dbt seed files from endpoints.yaml configuration.

This script solves the critical architectural flaw in generate_task_plan.sql
by creating dbt seed tables from the single source of truth (endpoints.yaml).

Usage:
    python scripts/generate_endpoint_seeds.py

Output:
    - dbt_baliza/seeds/endpoints_config.csv
    - Replaces hardcoded configuration in dbt macros
"""

import csv
import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

def load_endpoints_config(config_path: Path) -> dict:
    """Load and validate endpoints configuration."""
    try:
        with open(config_path, encoding='utf-8') as f:
            config = yaml.safe_load(f)

        if 'endpoints' not in config:
            raise ValueError("Missing 'endpoints' section in configuration")

        return config
    except Exception as e:
        logger.error(f"Failed to load endpoints config: {e}")
        raise

def generate_endpoints_seed(config: dict, output_path: Path):
    """Generate endpoints configuration seed file for dbt."""

    endpoints_data = []

    for endpoint_name, endpoint_config in config['endpoints'].items():
        # Convert modalidades list to string representation for CSV
        modalidades = endpoint_config.get('modalidades', [])
        modalidades_str = ','.join(map(str, modalidades)) if modalidades else ''

        endpoints_data.append({
            'endpoint_name': endpoint_name,
            'path': endpoint_config.get('path', ''),
            'description': endpoint_config.get('description', ''),
            'granularity': endpoint_config.get('granularity', 'month'),
            'page_size': endpoint_config.get('page_size', 500),
            'modalidades': modalidades_str,
            'active': endpoint_config.get('active', True),
            'supports_date_range': endpoint_config.get('supports_date_range', True),
            'date_params': ','.join(endpoint_config.get('date_params', [])),
        })

    # Write CSV file
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        if endpoints_data:
            fieldnames = endpoints_data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(endpoints_data)

    logger.info(f"Generated endpoints seed file: {output_path}")
    logger.info(f"Total endpoints: {len(endpoints_data)}")

def generate_modalidades_seed(config: dict, output_path: Path):
    """Generate modalidades seed file for enum mappings."""

    modalidades_data = []

    # Extract all unique modalidades from endpoints
    all_modalidades = set()
    for endpoint_config in config['endpoints'].values():
        modalidades = endpoint_config.get('modalidades', [])
        all_modalidades.update(modalidades)

    # Generate seed data (in production, this would come from Python enums)
    modalidade_names = {
        1: "Pregão",
        2: "Concorrência",
        3: "Tomada de Preços",
        4: "Convite",
        5: "Concurso",
        6: "Leilão",
        7: "Diálogo Competitivo",
        8: "Procedimento de Manifestação de Interesse",
        9: "Credenciamento",
        10: "Dispensa de Licitação",
        11: "Inexigibilidade",
        12: "Regime Diferenciado de Contratações Públicas",
        99: "Outros"
    }

    for modalidade_id in sorted(all_modalidades):
        modalidades_data.append({
            'modalidade_id': modalidade_id,
            'modalidade_nome': modalidade_names.get(modalidade_id, f'Modalidade {modalidade_id}'),
            'active': True
        })

    # Write CSV file
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        if modalidades_data:
            fieldnames = modalidades_data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(modalidades_data)

    logger.info(f"Generated modalidades seed file: {output_path}")
    logger.info(f"Total modalidades: {len(modalidades_data)}")

def main():
    """Main execution function."""
    logging.basicConfig(level=logging.INFO)

    # Define paths
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    config_path = project_root / "config" / "endpoints.yaml"
    seeds_dir = project_root / "dbt_baliza" / "seeds"

    endpoints_seed_path = seeds_dir / "endpoints_config.csv"
    modalidades_seed_path = seeds_dir / "modalidades_config.csv"

    try:
        # Load configuration
        logger.info(f"Loading configuration from: {config_path}")
        config = load_endpoints_config(config_path)

        # Generate seed files
        generate_endpoints_seed(config, endpoints_seed_path)
        generate_modalidades_seed(config, modalidades_seed_path)

        logger.info("\n✅ Seed file generation complete!")
        logger.info("Next steps:")
        logger.info("1. Run 'dbt seed' to load the configuration into DuckDB")
        logger.info("2. Update generate_task_plan.sql to use these seed tables")
        logger.info("3. Remove hardcoded configuration from the macro")

    except Exception as e:
        logger.error(f"Failed to generate seed files: {e}")
        return 1

    return 0

if __name__ == "__main__":
    exit(main())
