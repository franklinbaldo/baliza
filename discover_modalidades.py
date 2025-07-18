#!/usr/bin/env python3
"""
Script para descobrir todas as modalidades de contratação disponíveis
"""

import asyncio
import httpx
from datetime import date, timedelta

async def test_modalidade(modalidade_id: int) -> dict:
    """Testa uma modalidade específica."""
    test_date = (date.today() - timedelta(days=7)).strftime("%Y%m%d")
    
    params = {
        "dataInicial": test_date,
        "dataFinal": test_date,
        "codigoModalidadeContratacao": modalidade_id,
        "pagina": 1,
        "tamanhoPagina": 10,
    }
    
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "modalidade_id": modalidade_id,
                    "success": True,
                    "total_records": data.get("totalRegistros", 0),
                    "has_data": data.get("totalRegistros", 0) > 0,
                }
            else:
                return {
                    "modalidade_id": modalidade_id,
                    "success": False,
                    "status_code": response.status_code,
                    "error": response.text[:200],
                }
        except Exception as e:
            return {
                "modalidade_id": modalidade_id,
                "success": False,
                "error": str(e),
            }

async def discover_modalidades():
    """Descobre todas as modalidades válidas."""
    print("🔍 Descobrindo modalidades de contratação...")
    
    # Testa modalidades de 1 a 20 (cobertura inicial)
    modalidades_to_test = range(1, 21)
    
    results = []
    valid_modalidades = []
    
    for modalidade_id in modalidades_to_test:
        print(f"   Testando modalidade {modalidade_id}...")
        result = await test_modalidade(modalidade_id)
        results.append(result)
        
        if result["success"]:
            if result.get("has_data", False):
                print(f"   ✅ Modalidade {modalidade_id}: {result['total_records']} registros")
                valid_modalidades.append(modalidade_id)
            else:
                print(f"   ⚠️  Modalidade {modalidade_id}: Válida mas sem dados para esta data")
                valid_modalidades.append(modalidade_id)
        else:
            if result.get("status_code") == 422:
                print(f"   ❌ Modalidade {modalidade_id}: Inválida (422)")
            else:
                print(f"   💥 Modalidade {modalidade_id}: Erro {result.get('status_code', 'unknown')}")
        
        # Delay para ser respeitoso com a API
        await asyncio.sleep(0.5)
    
    print(f"\n📋 RESUMO:")
    print(f"✅ Modalidades válidas encontradas: {valid_modalidades}")
    print(f"📊 Total de modalidades válidas: {len(valid_modalidades)}")
    
    # Salva os resultados
    import json
    with open("modalidades_discovered.json", "w") as f:
        json.dump({
            "valid_modalidades": valid_modalidades,
            "test_results": results,
        }, f, indent=2)
    
    print(f"💾 Resultados salvos em: modalidades_discovered.json")
    
    return valid_modalidades

if __name__ == "__main__":
    asyncio.run(discover_modalidades())