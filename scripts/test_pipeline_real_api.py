#!/usr/bin/env python3
"""
Teste do pipeline BALIZA com a API PNCP real.
Usa um período pequeno (1 mês) para validar rapidamente.
"""

import dlt
from pathlib import Path
from src.baliza.pipeline import run_pipeline, baliza_source

def test_single_endpoint():
    """Testa um endpoint simples primeiro"""
    print("🧪 TESTANDO ENDPOINT SIMPLES (contratos/atualizacao)")
    print("=" * 60)
    
    try:
        # Cria pipeline simples
        pipeline = dlt.pipeline(
            pipeline_name="baliza_test_real",
            destination="duckdb",
            dataset_name="pncp_test_real",
            progress="log"
        )
        
        # Usa apenas specialized (2 recursos simples)
        resources_list = baliza_source(resource_type="specialized")
        
        print(f"📋 Resources a executar: {len(resources_list)}")
        for resource in resources_list:
            print(f"   - {resource.name}")
        
        # Executa apenas o primeiro resource para teste rápido
        first_resource = [resources_list[0]]  # Apenas o primeiro
        
        print(f"\n🚀 Executando apenas: {first_resource[0].name}")
        info = pipeline.run(first_resource)
        
        print(f"✅ Teste concluído!")
        print(f"📊 Jobs processados: {len(info.load_packages[0].jobs) if info.load_packages else 0}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro no teste: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_modalidade_endpoint():
    """Testa um endpoint que requer modalidade"""
    print("\n🧪 TESTANDO ENDPOINT COM MODALIDADE (contratacoes_atualizacao)")
    print("=" * 60)
    
    try:
        # Cria pipeline
        pipeline = dlt.pipeline(
            pipeline_name="baliza_test_modalidade",
            destination="duckdb", 
            dataset_name="pncp_test_modalidade",
            progress="log"
        )
        
        # Usa sync (inclui endpoints com modalidade)
        resources_list = baliza_source(resource_type="sync")
        
        print(f"📋 Total resources: {len(resources_list)}")
        
        # Pega apenas um resource de modalidade para teste rápido
        modalidade_resources = [r for r in resources_list if "_mod" in r.name]
        if modalidade_resources:
            test_resource = [modalidade_resources[0]]  # Apenas o primeiro
            
            print(f"🚀 Executando apenas: {test_resource[0].name}")
            info = pipeline.run(test_resource)
            
            print(f"✅ Teste modalidade concluído!")
            print(f"📊 Jobs processados: {len(info.load_packages[0].jobs) if info.load_packages else 0}")
            
            return True
        else:
            print("❌ Nenhum resource de modalidade encontrado")
            return False
            
    except Exception as e:
        print(f"❌ Erro no teste modalidade: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_chunking():
    """Testa se o chunking mensal funciona corretamente"""
    print("\n🧪 TESTANDO CHUNKING MENSAL")
    print("=" * 60)
    
    from src.baliza.pipeline import split_date_range_monthly
    
    # Testa chunking de vários meses
    chunks = split_date_range_monthly("20240101", "20240331")  # 3 meses
    
    print(f"📅 Período: 2024-01-01 até 2024-03-31")
    print(f"📦 Chunks gerados: {len(chunks)}")
    
    for i, (start, end) in enumerate(chunks, 1):
        print(f"   {i}. {start} até {end}")
    
    # Deve ter 3 chunks (Jan, Fev, Mar)
    expected = 3
    if len(chunks) == expected:
        print(f"✅ Chunking correto: {len(chunks)} chunks")
        return True
    else:
        print(f"❌ Chunking incorreto: esperado {expected}, obtido {len(chunks)}")
        return False

def main():
    """Executa todos os testes"""
    print("🚀 INICIANDO TESTES COM API PNCP REAL")
    print("="*60)
    
    # Cria diretórios necessários
    Path("data").mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    
    tests_passed = 0
    total_tests = 3
    
    # Teste 1: Chunking
    if test_chunking():
        tests_passed += 1
    
    # Teste 2: Endpoint simples
    if test_single_endpoint():
        tests_passed += 1
    
    # Teste 3: Endpoint com modalidade  
    if test_modalidade_endpoint():
        tests_passed += 1
    
    print(f"\n" + "="*60)
    print(f"🎯 RESUMO DOS TESTES")
    print(f"="*60)
    print(f"✅ Testes passaram: {tests_passed}/{total_tests}")
    
    if tests_passed == total_tests:
        print("🎉 TODOS OS TESTES PASSARAM!")
        print("✅ Pipeline BALIZA funcionando com API PNCP real!")
        print("\n🚀 PRONTO PARA EXECUÇÃO COMPLETA!")
    else:
        print("⚠️  Alguns testes falharam. Verifique os logs acima.")
    
    return tests_passed == total_tests

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)