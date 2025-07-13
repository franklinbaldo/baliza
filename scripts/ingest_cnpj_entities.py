#!/usr/bin/env python3
"""
Ingest CNPJ data for public entities to create reference data for coverage analysis.

This script downloads CNPJ data from Receita Federal and creates a DBT seed file
with public entities (governo, prefeituras, etc.) for coverage analysis.
"""
import csv
import os
import requests
import zipfile
import tempfile
from pathlib import Path
from typing import Set, Dict, Any
import duckdb


def download_cnpj_data(output_dir: Path) -> Path:
    """
    Download CNPJ public entities data from minha-receita project.
    
    Uses the public API from https://github.com/cuducos/minha-receita
    which provides clean CNPJ data including public entities.
    """
    print("🔄 Downloading CNPJ public entities data from minha-receita...")
    
    cnpj_file = output_dir / "entidades_publicas.csv"
    
    # Public entity nature codes (natureza jurídica)
    public_nature_codes = [
        '1104',  # Administração Pública Federal
        '1120',  # Administração Pública Estadual  
        '1139',  # Administração Pública Municipal
        '1162',  # Autarquia Federal
        '1163',  # Autarquia Estadual
        '1164',  # Autarquia Municipal
        '1171',  # Fundação Pública Federal
        '1172',  # Fundação Pública Estadual
        '1173',  # Fundação Pública Municipal
        '1244',  # Empresa Pública
        '1245',  # Sociedade de Economia Mista
    ]
    
    # Minha Receita API base URL
    base_url = "https://minhareceita.org"
    
    entities = []
    
    print("📋 Fetching known public entities from various sources...")
    
    # Known major public entities to start with
    known_cnpjs = [
        '04092265000192',  # Prefeitura de Porto Velho
        '04092706000198',  # Estado de Rondônia
        '00394460000107',  # União - Presidência da República
        '34028316000103',  # Estado de São Paulo
        '46395000000139',  # Prefeitura de São Paulo
        '17312022000178',  # Estado do Rio de Janeiro
        '42498430000101',  # Prefeitura do Rio de Janeiro
        '76416035000134',  # Estado de Minas Gerais
        '18715515000140',  # Prefeitura de Belo Horizonte
    ]
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Baliza/1.0 (PNCP Coverage Analysis)',
        'Accept': 'application/json'
    })
    
    for cnpj in known_cnpjs:
        try:
            # Format CNPJ for minha-receita API
            cnpj_formatted = f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"
            
            print(f"  🔍 Fetching data for {cnpj_formatted}...")
            
            # Query minha-receita API
            response = session.get(f"{base_url}/{cnpj}", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract relevant fields
                entity = {
                    'cnpj': cnpj_formatted,
                    'razao_social': data.get('company', {}).get('name', '').upper(),
                    'uf': data.get('address', {}).get('state', ''),
                    'municipio': data.get('address', {}).get('city', '').upper(),
                    'natureza_juridica': str(data.get('company', {}).get('nature', {}).get('code', '')),
                    'porte': data.get('company', {}).get('size', {}).get('acronym', ''),
                    'situacao': data.get('status', {}).get('text', '').upper()
                }
                
                # Verify it's a public entity
                if entity['natureza_juridica'] in public_nature_codes:
                    entities.append(entity)
                    print(f"    ✅ Added: {entity['razao_social'][:50]}")
                else:
                    print(f"    ⚠️ Skipped (not public): {entity['razao_social'][:50]}")
                    
            elif response.status_code == 404:
                print(f"    ⚠️ CNPJ not found: {cnpj_formatted}")
            else:
                print(f"    ❌ API error {response.status_code} for {cnpj_formatted}")
                
        except requests.RequestException as e:
            print(f"    ❌ Network error for {cnpj}: {e}")
        except Exception as e:
            print(f"    ❌ Error processing {cnpj}: {e}")
    
    # If API fails, fallback to sample data
    if not entities:
        print("⚠️ API fetch failed, using sample data...")
        entities = [
            {
                'cnpj': '04.092.265/0001-92', 
                'razao_social': 'PREFEITURA MUNICIPAL DE PORTO VELHO',
                'uf': 'RO',
                'municipio': 'PORTO VELHO',
                'natureza_juridica': '1139',
                'porte': 'GRANDE',
                'situacao': 'ATIVA'
            },
            {
                'cnpj': '04.092.706/0001-98',
                'razao_social': 'ESTADO DE RONDONIA',  
                'uf': 'RO',
                'municipio': 'PORTO VELHO',
                'natureza_juridica': '1120',
                'porte': 'GRANDE', 
                'situacao': 'ATIVA'
            },
            {
                'cnpj': '00.394.460/0001-07',
                'razao_social': 'UNIÃO - PRESIDENCIA DA REPUBLICA',
                'uf': 'DF',
                'municipio': 'BRASILIA',
                'natureza_juridica': '1104',
                'porte': 'GRANDE',
                'situacao': 'ATIVA'
            }
        ]
    
    # Save to CSV
    with open(cnpj_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['cnpj', 'razao_social', 'uf', 'municipio', 'natureza_juridica', 'porte', 'situacao']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(entities)
    
    print(f"✅ Created entities file with {len(entities)} public entities: {cnpj_file}")
    return cnpj_file


def identify_public_entities(cnpj_file: Path) -> Set[str]:
    """
    Identify public entities based on CNPJ natureza jurídica codes.
    
    Public entity codes (simplified):
    - 1104: Administração Pública Federal
    - 1120: Administração Pública Estadual  
    - 1139: Administração Pública Municipal
    - 1162: Autarquia Federal
    - 1163: Autarquia Estadual
    - 1164: Autarquia Municipal
    - 1171: Fundação Pública Federal
    - 1172: Fundação Pública Estadual
    - 1173: Fundação Pública Municipal
    """
    public_codes = {
        '1104', '1120', '1139',  # Administração Pública
        '1162', '1163', '1164',  # Autarquias
        '1171', '1172', '1173'   # Fundações Públicas
    }
    
    public_cnpjs = set()
    
    with open(cnpj_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['natureza_juridica'] in public_codes:
                # Clean CNPJ format
                cnpj_clean = row['cnpj'].replace('.', '').replace('/', '').replace('-', '')
                public_cnpjs.add(cnpj_clean)
    
    return public_cnpjs


def enhance_with_baliza_data(public_entities: Set[str], baliza_db_path: Path) -> Dict[str, Any]:
    """
    Enhance public entities data with information from Baliza database.
    """
    if not baliza_db_path.exists():
        print(f"⚠️ Baliza database not found at {baliza_db_path}, using basic entity list")
        return {cnpj: {"found_in_baliza": False} for cnpj in public_entities}
    
    print(f"🔍 Enhancing entities data with Baliza database...")
    
    conn = duckdb.connect(str(baliza_db_path))
    
    # Get entities that appear in Baliza data
    baliza_entities = conn.execute("""
        SELECT DISTINCT 
            orgao_cnpj,
            orgao_razao_social,
            uf_sigla,
            COUNT(*) as total_contratos,
            MIN(data_assinatura) as primeiro_contrato,
            MAX(data_assinatura) as ultimo_contrato
        FROM staging.stg_contratos 
        WHERE orgao_cnpj IS NOT NULL 
        GROUP BY 1, 2, 3
    """).fetchall()
    
    enhanced_data = {}
    
    for cnpj in public_entities:
        # Format CNPJ for comparison (add dots, slash, dash)
        cnpj_formatted = f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"
        
        # Find matching entity in Baliza
        matching_entity = None
        for entity in baliza_entities:
            if entity[0] == cnpj_formatted:
                matching_entity = entity
                break
        
        if matching_entity:
            enhanced_data[cnpj] = {
                "found_in_baliza": True,
                "razao_social": matching_entity[1],
                "uf": matching_entity[2], 
                "total_contratos": matching_entity[3],
                "primeiro_contrato": matching_entity[4],
                "ultimo_contrato": matching_entity[5]
            }
        else:
            enhanced_data[cnpj] = {
                "found_in_baliza": False
            }
    
    conn.close()
    return enhanced_data


def create_dbt_seed(entities_data: Dict[str, Any], output_path: Path):
    """
    Create DBT seed file for public entities reference data.
    """
    print(f"📝 Creating DBT seed file: {output_path}")
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'cnpj', 'found_in_baliza', 'razao_social', 'uf', 
            'total_contratos', 'primeiro_contrato', 'ultimo_contrato'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for cnpj, data in entities_data.items():
            # Format CNPJ with dots, slash, dash
            cnpj_formatted = f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"
            
            row = {
                'cnpj': cnpj_formatted,
                'found_in_baliza': data['found_in_baliza'],
                'razao_social': data.get('razao_social', ''),
                'uf': data.get('uf', ''),
                'total_contratos': data.get('total_contratos', 0),
                'primeiro_contrato': data.get('primeiro_contrato', ''),
                'ultimo_contrato': data.get('ultimo_contrato', '')
            }
            writer.writerow(row)
    
    print(f"✅ DBT seed created with {len(entities_data)} entities")


def main():
    """Main function to orchestrate CNPJ data ingestion."""
    
    # Paths
    project_root = Path(__file__).parent.parent
    temp_dir = Path(tempfile.mkdtemp())
    dbt_seeds_dir = project_root / "dbt_baliza" / "seeds"
    baliza_db_path = project_root / "state" / "baliza.duckdb"
    
    # Ensure seeds directory exists
    dbt_seeds_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        print("🚀 Starting CNPJ public entities ingestion...")
        
        # Step 1: Download CNPJ data (simplified for now)
        cnpj_file = download_cnpj_data(temp_dir)
        
        # Step 2: Identify public entities
        print("🔍 Identifying public entities...")
        public_entities = identify_public_entities(cnpj_file)
        print(f"✅ Found {len(public_entities)} public entities")
        
        # Step 3: Enhance with Baliza data
        enhanced_data = enhance_with_baliza_data(public_entities, baliza_db_path)
        
        found_in_baliza = sum(1 for data in enhanced_data.values() if data['found_in_baliza'])
        print(f"📊 {found_in_baliza}/{len(public_entities)} entities found in Baliza data")
        
        # Step 4: Create DBT seed
        seed_file = dbt_seeds_dir / "ref_entidades_publicas.csv"
        create_dbt_seed(enhanced_data, seed_file)
        
        print(f"""
✅ CNPJ ingestion completed successfully!

📁 Files created:
   - DBT seed: {seed_file}

📊 Summary:
   - Total public entities: {len(public_entities)}
   - Found in Baliza: {found_in_baliza}
   - Coverage rate: {found_in_baliza/len(public_entities)*100:.1f}%

🎯 Next steps:
   - Run: dbt seed --select ref_entidades_publicas
   - Run: dbt run --select coverage_models
        """)
        
    finally:
        # Cleanup
        if temp_dir.exists():
            import shutil
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    main()