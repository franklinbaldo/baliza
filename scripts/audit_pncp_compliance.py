import re
import json
from pathlib import Path

def extract_enums_from_manual(manual_path: Path) -> dict:
    """Extrai tabelas de domínio (ENUMs) do manual em markdown."""
    text = manual_path.read_text(encoding='utf-8')
    enums = {}

    # Regex para encontrar seções de tabelas de domínio
    # O padrão foi ajustado para lidar com formatos de tabela e listas com marcadores
    # Regex para capturar todas as seções de domínio
    pattern = re.compile(r"###\s+5\.(\d{1,2})\.\s+([^\n]+)(.*?)(?=\n###\s+5\.|\n##\s+6\.)", re.DOTALL)

    list_pattern = re.compile(r"\*\s+\(código\s*=\s*(\d+)\)\s+(.+)")
    natureza_juridica_pattern = re.compile(r"\*\s+(\d{4})\s+-\s+(.+)")

    matches = pattern.findall(text)

    # Adiciona a última seção manualmente
    last_section_match = re.search(r"###\s+5\.17\.\s+([^\n]+)(.*?)(?=\n##\s+7\.)", text, re.DOTALL)
    if last_section_match and not any(m[0] == '17' for m in matches):
        matches.append(('17', last_section_match.group(1), last_section_match.group(2)))


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
    """Parseia o schema OpenAPI para extrair tipos de dados."""
    with schema_path.open('r', encoding='utf-8') as f:
        schema = json.load(f)

    # Aqui você extrairia as definições de schema relevantes
    # Por simplicidade, vamos focar em um exemplo
    # A implementação completa dependeria da estrutura do JSON

    return schema.get('components', {}).get('schemas', {})

def generate_compliance_report(enums: dict, openapi_schemas: dict, output_path: Path):
    """Gera o relatório de auditoria de compliance."""
    report = []
    report.append("# Relatório de Auditoria de Compliance PNCP\n")

    report.append("## 1. Tabelas de Domínio (ENUMs) Encontradas\n")
    for name, values in enums.items():
        report.append(f"### {name}")
        report.append(f"Encontrados {len(values)} valores.")
        # Preview dos primeiros 3 valores
        for value in values[:3]:
            report.append(f"- `{value}`")
        report.append("\n")

    report.append("## 2. Análise do Schema OpenAPI\n")
    report.append(f"Encontrados {len(openapi_schemas)} schemas no OpenAPI.\n")

    # Exemplo de análise - comparar com um schema hipotético do Baliza
    # baliza_schema = {"Contratacao": {"properties": {"modalidadeId": "integer"}}}
    # if "Contratacao" in openapi_schemas:
    #     official_type = openapi_schemas["Contratacao"]["properties"]["modalidadeId"]["type"]
    #     report.append(f"Comparação para Contratacao.modalidadeId:")
    #     report.append(f"  - Schema Oficial: {official_type}")
    #     report.append(f"  - Schema Baliza (hipotético): {baliza_schema['Contratacao']['properties']['modalidadeId']}")

    output_path.write_text("\n".join(report), encoding='utf-8')

def main():
    """Função principal para executar a auditoria."""
    repo_root = Path(__file__).parent.parent

    # Caminhos para os arquivos de documentação
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

if __name__ == "__main__":
    main()
