import re
import json
import csv
from pathlib import Path
from prance import ResolvingParser
from prance.util.formats import ParseError

def extract_enums_from_manual(manual_path: Path) -> dict:
    """Extrai tabelas de domínio (ENUMs) do manual em markdown."""
    text = manual_path.read_text(encoding='utf-8')
    enums = {}

    # Regex para encontrar seções de tabelas de domínio
    # O padrão foi ajustado para lidar com formatos de tabela e listas com marcadores
    # Regex aprimorado para capturar todas as 17 seções, incluindo a última.
    # O lookahead agora inclui o final do texto (`\Z`) como um delimitador.
    pattern = re.compile(r"###\s+5\.(\d{1,2})\.\s+([^\n]+)(.*?)(?=\n###\s+5\.|\n##\s+6\.|\Z)", re.DOTALL)

    list_pattern = re.compile(r"\*\s+\(código\s*=\s*(\d+)\)\s+(.+)")
    natureza_juridica_pattern = re.compile(r"\*\s+(\d{4})\s+-\s+(.+)")

    matches = pattern.findall(text)

    for section_number, enum_name, content in matches:
        enum_name = enum_name.strip()
        content = content.strip()
        values = []

        if section_number == '13':  # Natureza Jurídica
            for item_match in natureza_juridica_pattern.finditer(content):
                values.append({"código": item_match.group(1).strip(), "descrição": item_match.group(2).strip()})
        elif section_number == '12': # Tipo de Documento
            # Extrair os três tipos de documentos
            doc_types = re.split(r'\*\*Tipos de documentos d[a-z]s?\s+[^:]+:\*\*', content)
            for doc_type_content in doc_types:
                if not doc_type_content.strip(): continue
                for item_match in list_pattern.finditer(doc_type_content):
                     values.append({"código": item_match.group(1).strip(), "descrição": item_match.group(2).strip().split(':')[0].strip()})
        elif section_number != '17':  # Outras seções, exceto a 17
            for item_match in list_pattern.finditer(content):
                values.append({"código": item_match.group(1).strip(), "descrição": item_match.group(2).strip().split(':')[0].strip()})

        if values:
            enums[enum_name] = values

    return enums

def parse_openapi_schema(schema_path: Path) -> dict:
    """Parseia o schema OpenAPI usando Prance para uma análise detalhada."""
    try:
        # Prance resolve todas as referências ($ref) automaticamente
        parser = ResolvingParser(str(schema_path))

        # Extrai os schemas dos componentes
        components = parser.specification.get('components', {})
        schemas = components.get('schemas', {})

        # Realiza uma análise detalhada por schema
        schema_analysis = {}
        for name, schema_def in schemas.items():
            properties = schema_def.get('properties', {})
            schema_analysis[name] = {
                'type': schema_def.get('type'),
                'properties': list(properties.keys()),
                'required': schema_def.get('required', []),
                'property_types': {
                    prop: prop_def.get('type', 'unknown')
                    for prop, prop_def in properties.items()
                }
            }

        return schema_analysis

    except ParseError as e:
        print(f"❌ Erro de parsing no OpenAPI com Prance: {e}")
        return {}
    except Exception as e:
        print(f"❌ Erro inesperado durante o parsing do OpenAPI: {e}")
        return {}

def generate_compliance_report(enums: dict, openapi_analysis: dict, output_path: Path):
    """Gera um relatório de compliance detalhado, incluindo a análise do Prance."""
    report = []
    report.append("# Relatório de Auditoria de Compliance PNCP\n")

    # Seção 1: ENUMs
    report.append("## 1. Tabelas de Domínio (ENUMs) Encontradas\n")
    for name, values in enums.items():
        report.append(f"### {name}")
        report.append(f"Encontrados {len(values)} valores.")
        for value in values[:3]:
            report.append(f"- `{value['código']}`: {value['descrição']}")
        if len(values) > 3:
            report.append(f"... e mais {len(values) - 3} valores\n")

    # Seção 2: Análise detalhada do OpenAPI
    report.append("## 2. Análise Detalhada do Schema OpenAPI (via Prance)\n")
    for schema_name, schema_info in openapi_analysis.items():
        report.append(f"### Schema: `{schema_name}`")
        report.append(f"- **Tipo**: `{schema_info.get('type', 'N/A')}`")
        report.append(f"- **Total de Propriedades**: {len(schema_info.get('properties', []))}")
        report.append(f"- **Campos Obrigatórios**: {len(schema_info.get('required', []))}")

        if schema_info.get('required'):
            report.append("\n**Campos obrigatórios e seus tipos:**")
            for field in schema_info['required']:
                field_type = schema_info.get('property_types', {}).get(field, 'unknown')
                report.append(f"  - `{field}`: `{field_type}`")
            report.append("")

    # Seção 3: Mapeamento ENUMs vs. OpenAPI
    report.append("## 3. Mapeamento Preliminar: ENUMs vs. Campos OpenAPI\n")
    enum_mapping = {
        'Modalidade de Contratação': 'codigoModalidadeContratacao',
        'Situação da Contratação': 'situacaoCompraId', # Nome do campo pode variar, verificar no schema
        'Amparo Legal': 'amparoLegal',
    }
    for enum_name, openapi_field in enum_mapping.items():
        if enum_name in enums:
            report.append(f"- ✅ **{enum_name}** → `{openapi_field}`: ENUM encontrado com {len(enums[enum_name])} valores.")
        else:
            report.append(f"- ❌ **{enum_name}** → `{openapi_field}`: ENUM não encontrado no manual.")

    output_path.write_text("\n".join(report), encoding='utf-8')

def generate_sql_seeds(enums: dict, output_path: Path):
    """Gera um arquivo SQL com os dados das tabelas de domínio para o dbt seeds."""
    if not output_path.parent.exists():
        output_path.parent.mkdir(parents=True)

    # Mapeamento para nomes em inglês padronizados
    english_names = {
        'Instrumento Convocatório': 'call_instrument',
        'Modalidade de Contratação': 'contracting_modality', 
        'Modo de Disputa': 'bidding_mode',
        'Critério de Julgamento': 'judgment_criteria'
    }

    with output_path.open('w', encoding='utf-8') as f:
        for table_name, values in enums.items():
            # Usa nome em inglês se disponível, senão normaliza
            safe_table_name = english_names.get(table_name, re.sub(r'\W|^(?=\d)', '_', table_name.lower()))
            f.write(f"-- Seeds para a tabela de domínio: {table_name}\n")

            # Cria um CSV inline com colunas em inglês
            f.write(f"code,description\n")
            for value in values:
                # Escapa aspas duplas na descrição e a coloca entre aspas
                description = '"' + value['descrição'].replace('"', '""') + '"'
                f.write(f"{value['código']},{description}\n")
            f.write("\n")


def main():
    """Função principal para executar a auditoria e gerar os seeds."""
    repo_root = Path(__file__).parent.parent

    # Caminhos
    manual_path = repo_root / "docs/openapi/MANUAL-PNCP-CONSULSTAS-VERSAO-1.md"
    openapi_path = repo_root / "docs/openapi/api-pncp-consulta.json"
    report_path = repo_root / "docs/pncp-compliance-audit.md"

    print("1. Extraindo ENUMs do manual...")
    enums = extract_enums_from_manual(manual_path)
    print(f"   Encontradas {len(enums)} tabelas de domínio.")

    print("2. Parseando schema OpenAPI...")
    openapi_schemas = parse_openapi_schema(openapi_path)
    print(f"   Encontrados {len(openapi_schemas)} schemas.")

    print("3. Gerando relatório de compliance...")
    generate_compliance_report(enums, openapi_schemas, report_path)
    print(f"   Relatório salvo em: {report_path}")

    print("4. Gerando arquivos de seeds CSV para dbt...")
    seeds_dir = repo_root / "dbt_baliza/seeds/domain_tables"
    if not seeds_dir.exists():
        seeds_dir.mkdir(parents=True)

    # Mapeamento para nomes em inglês padronizados
    english_names = {
        'Instrumento Convocatório': 'call_instrument',
        'Modalidade de Contratação': 'contracting_modality', 
        'Modo de Disputa': 'bidding_mode',
        'Critério de Julgamento': 'judgment_criteria'
    }

    for table_name, values in enums.items():
        # Usa nome em inglês se disponível, senão normaliza
        safe_table_name = english_names.get(table_name, re.sub(r'\W|^(?=\d)', '_', table_name.lower()))
        seed_path = seeds_dir / f"{safe_table_name}.csv"
        try:
            with seed_path.open('w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['code', 'description'])  # Colunas em inglês
                for value in values:
                    writer.writerow([value['código'], value['descrição']])
            print(f"   Seed CSV salvo em: {seed_path}")
        except OSError as e:
            print(f"Erro ao salvar o arquivo {seed_path}: {e}")


if __name__ == "__main__":
    main()
