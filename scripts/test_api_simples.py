#!/usr/bin/env python3
"""
Teste simples da API PNCP real para validar se funciona.
"""

import requests


def test_pncp_api():
    """Testa a API PNCP real com o endpoint fornecido pelo usuário"""
    print("🧪 TESTANDO API PNCP REAL")
    print("=" * 50)

    # URL fornecida pelo usuário
    url = "https://pncp.gov.br/api/consulta/v1/contratos/atualizacao"
    params = {
        "tamanhoPagina": 10,  # Pequeno para teste
        "dataInicial": "20210101",
        "dataFinal": "20211231",
        "pagina": 1,
    }

    print(f"🔄 Fazendo requisição para: {url}")
    print(f"📋 Parâmetros: {params}")

    try:
        response = requests.get(url, params=params, timeout=30)
        print(f"📊 Status: {response.status_code}")

        if response.status_code == 200:
            data = response.json()

            print("✅ Resposta recebida com sucesso!")
            print("📋 Estrutura da resposta:")
            print(
                f"   - Keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}"
            )

            if isinstance(data, dict):
                if "data" in data:
                    items = data["data"]
                    print(f"   📊 Total items: {len(items)}")
                    print(f"   📄 Total páginas: {data.get('totalPaginas', 'N/A')}")
                    print(f"   🔢 Página atual: {data.get('numeroPagina', 'N/A')}")

                    if items and len(items) > 0:
                        print(f"   📋 Primeiro item keys: {list(items[0].keys())}")
                        print(f"   📝 Exemplo: {items[0]}")

                    return True
                else:
                    print("   ❌ Campo 'data' não encontrado na resposta")
                    print(f"   📄 Resposta: {data}")
            else:
                print(f"   ❌ Resposta não é um dict: {type(data)}")
        else:
            print(f"❌ Erro HTTP: {response.status_code}")
            print(f"📄 Resposta: {response.text[:500]}")

    except Exception as e:
        print(f"❌ Erro na requisição: {e}")
        return False

    return False


def test_modalidade_endpoint():
    """Testa endpoint que requer modalidade"""
    print("\n🧪 TESTANDO ENDPOINT COM MODALIDADE")
    print("=" * 50)

    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    params = {
        "tamanhoPagina": 10,  # Mínimo é 10
        "dataInicial": "20210101",
        "dataFinal": "20211231",  # Ano inteiro para ter mais dados
        "codigoModalidadeContratacao": 6,  # Pregão Eletrônico (mais comum)
        "pagina": 1,
    }

    print(f"🔄 Fazendo requisição para: {url}")
    print(f"📋 Parâmetros: {params}")

    try:
        response = requests.get(url, params=params, timeout=30)
        print(f"📊 Status: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            print("✅ Endpoint com modalidade funcionando!")

            if isinstance(data, dict) and "data" in data:
                items = data["data"]
                print(f"   📊 Items encontrados: {len(items)}")
                return True
        elif response.status_code == 204:
            print("✅ Endpoint funcionando (sem dados para o período/modalidade)")
            return True  # 204 é sucesso, apenas sem conteúdo
        else:
            print(f"❌ Erro: {response.status_code}")
            print(f"📄 Resposta: {response.text[:300]}")

    except Exception as e:
        print(f"❌ Erro: {e}")

    return False


def main():
    """Executa testes da API"""
    print("🚀 VALIDANDO API PNCP REAL")
    print("=" * 60)

    tests_passed = 0

    # Teste 1: Endpoint simples
    if test_pncp_api():
        tests_passed += 1

    # Teste 2: Endpoint com modalidade
    if test_modalidade_endpoint():
        tests_passed += 1

    print("\n" + "=" * 60)
    print("🎯 RESUMO")
    print(f"✅ Testes passaram: {tests_passed}/2")

    if tests_passed == 2:
        print("🎉 API PNCP FUNCIONANDO!")
        print("✅ Pronto para integrar no pipeline DLT")
    else:
        print("⚠️  Problemas com a API. Verificar conectividade.")

    return tests_passed == 2


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
