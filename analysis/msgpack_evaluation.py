"""
MessagePack vs Current (JSON + zlib) Analysis for Baliza Raw Storage
"""

import json
import zlib
import msgpack
import time
import sys
from typing import Dict, Any
import hashlib

# TODO: This script is for analysis and evaluation purposes only. It is not part
#       of the production data pipeline and should not be executed as part of
#       any automated workflow. Its purpose is to compare serialization methods,
#       and its findings should be used to inform design decisions. Consider
#       moving this script to a dedicated `benchmarks` or `experiments` directory
#       to further clarify its role outside the main application.

def create_sample_pncp_payload() -> Dict[str, Any]:
    """Create a realistic PNCP API response payload for testing"""
    return {
        "data": [
            {
                "anoCompra": 2024,
                "sequencialCompra": 12345,
                "numeroCompra": "001/2024",
                "numeroControlePNCP": "12345678901234567890",
                "orgaoEntidade": {
                    "cnpj": "12.345.678/0001-90",
                    "razaoSocial": "Prefeitura Municipal de Exemplo",
                    "poderId": "E",
                    "esferaId": "M",
                },
                "unidadeOrgao": {
                    "codigoUnidade": "001",
                    "nomeUnidade": "Secretaria de Administração",
                    "ufSigla": "SP",
                    "municipioNome": "São Paulo",
                    "codigoIbge": "3550308",
                },
                "modalidadeId": 6,
                "modalidadeNome": "Pregão - Eletrônico",
                "modoDisputaId": 1,
                "modoDisputaNome": "Aberto",
                "objetoCompra": "Aquisição de material de escritório para as atividades administrativas",
                "valorTotalEstimado": 50000.00,
                "valorTotalHomologado": 48500.00,
                "dataPublicacaoPncp": "2024-01-15T10:30:00",
                "dataAberturaProposta": "2024-01-20T09:00:00",
                "dataEncerramentoProposta": "2024-01-20T17:00:00",
                "situacaoCompraId": "1",
                "situacaoCompraNome": "Divulgada no PNCP",
                "srp": False,
                "informacaoComplementar": "Processo administrativo nº 2024/001",
                "linkSistemaOrigem": "https://compras.exemplo.gov.br/processo/12345",
                "fontesOrcamentarias": [
                    {
                        "codigo": 100,
                        "nome": "Recursos Ordinários",
                        "descricao": "Receitas correntes ordinárias",
                    }
                ],
            },
            # Duplicate the structure to simulate multiple records
            {
                "anoCompra": 2024,
                "sequencialCompra": 12346,
                "numeroCompra": "002/2024",
                "numeroControlePNCP": "12345678901234567891",
                "orgaoEntidade": {
                    "cnpj": "98.765.432/0001-10",
                    "razaoSocial": "Governo do Estado de Exemplo",
                    "poderId": "E",
                    "esferaId": "E",
                },
                "unidadeOrgao": {
                    "codigoUnidade": "002",
                    "nomeUnidade": "Secretaria de Saúde",
                    "ufSigla": "RJ",
                    "municipioNome": "Rio de Janeiro",
                    "codigoIbge": "3304557",
                },
                "modalidadeId": 8,
                "modalidadeNome": "Dispensa de Licitação",
                "objetoCompra": "Aquisição emergencial de medicamentos para atendimento à população",
                "valorTotalEstimado": 125000.00,
                "valorTotalHomologado": 125000.00,
                "dataPublicacaoPncp": "2024-01-16T14:45:00",
                "situacaoCompraId": "1",
                "situacaoCompraNome": "Divulgada no PNCP",
                "srp": True,
                "informacaoComplementar": "Dispensa fundamentada no art. 24, IV da Lei 8.666/93",
                "fontesOrcamentarias": [
                    {
                        "codigo": 200,
                        "nome": "Recursos de Convênios",
                        "descricao": "Recursos federais transferidos via convênio",
                    },
                    {
                        "codigo": 300,
                        "nome": "Recursos Estaduais",
                        "descricao": "Recursos próprios do estado",
                    },
                ],
            },
        ],
        "totalRegistros": 2,
        "totalPaginas": 1,
        "numeroPagina": 1,
        "paginasRestantes": 0,
        "empty": False,
    }


def benchmark_serialization_methods(payload: Dict[str, Any], iterations: int = 1000):
    """Benchmark different serialization and compression methods"""

    print(f"🧪 Benchmarking serialization methods ({iterations} iterations)")
    print("=" * 60)

    results = {}

    # Method 1: Current approach (JSON + zlib)
    print("1️⃣  Current: JSON + zlib compression")
    start_time = time.time()
    json_zlib_sizes = []

    for _ in range(iterations):
        # Serialize to JSON
        json_bytes = json.dumps(
            payload, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        # Compress with zlib
        compressed = zlib.compress(json_bytes, level=6)
        json_zlib_sizes.append(len(compressed))

    json_zlib_time = time.time() - start_time
    avg_json_zlib_size = sum(json_zlib_sizes) / len(json_zlib_sizes)

    results["json_zlib"] = {
        "serialization_time": json_zlib_time,
        "avg_size": avg_json_zlib_size,
        "original_json_size": len(json_bytes),
    }

    print(f"   ⏱️  Time: {json_zlib_time:.4f}s")
    print(f"   📏 Original JSON size: {len(json_bytes):,} bytes")
    print(f"   📦 Compressed size: {avg_json_zlib_size:,.0f} bytes")
    print(f"   📊 Compression ratio: {len(json_bytes) / avg_json_zlib_size:.2f}x")
    print()

    # Method 2: MessagePack (no additional compression)
    print("2️⃣  MessagePack (no compression)")
    start_time = time.time()
    msgpack_sizes = []

    for _ in range(iterations):
        # Serialize to MessagePack
        msgpack_bytes = msgpack.packb(payload, use_bin_type=True)
        msgpack_sizes.append(len(msgpack_bytes))

    msgpack_time = time.time() - start_time
    avg_msgpack_size = sum(msgpack_sizes) / len(msgpack_sizes)

    results["msgpack"] = {
        "serialization_time": msgpack_time,
        "avg_size": avg_msgpack_size,
    }

    print(f"   ⏱️  Time: {msgpack_time:.4f}s")
    print(f"   📦 Size: {avg_msgpack_size:,.0f} bytes")
    print(f"   📊 vs JSON: {len(json_bytes) / avg_msgpack_size:.2f}x smaller")
    print()

    # Method 3: MessagePack + zlib compression
    print("3️⃣  MessagePack + zlib compression")
    start_time = time.time()
    msgpack_zlib_sizes = []

    for _ in range(iterations):
        # Serialize to MessagePack
        msgpack_bytes = msgpack.packb(payload, use_bin_type=True)
        # Compress with zlib
        compressed = zlib.compress(msgpack_bytes, level=6)
        msgpack_zlib_sizes.append(len(compressed))

    msgpack_zlib_time = time.time() - start_time
    avg_msgpack_zlib_size = sum(msgpack_zlib_sizes) / len(msgpack_zlib_sizes)

    results["msgpack_zlib"] = {
        "serialization_time": msgpack_zlib_time,
        "avg_size": avg_msgpack_zlib_size,
    }

    print(f"   ⏱️  Time: {msgpack_zlib_time:.4f}s")
    print(f"   📦 Compressed size: {avg_msgpack_zlib_size:,.0f} bytes")
    print(f"   📊 vs JSON+zlib: {avg_json_zlib_size / avg_msgpack_zlib_size:.2f}x")
    print()

    # Method 4: MessagePack + zlib level 9 (max compression)
    print("4️⃣  MessagePack + zlib level 9 (max compression)")
    start_time = time.time()
    msgpack_zlib9_sizes = []

    for _ in range(iterations):
        msgpack_bytes = msgpack.packb(payload, use_bin_type=True)
        compressed = zlib.compress(msgpack_bytes, level=9)
        msgpack_zlib9_sizes.append(len(compressed))

    msgpack_zlib9_time = time.time() - start_time
    avg_msgpack_zlib9_size = sum(msgpack_zlib9_sizes) / len(msgpack_zlib9_sizes)

    results["msgpack_zlib9"] = {
        "serialization_time": msgpack_zlib9_time,
        "avg_size": avg_msgpack_zlib9_size,
    }

    print(f"   ⏱️  Time: {msgpack_zlib9_time:.4f}s")
    print(f"   📦 Compressed size: {avg_msgpack_zlib9_size:,.0f} bytes")
    print(f"   📊 vs JSON+zlib: {avg_json_zlib_size / avg_msgpack_zlib9_size:.2f}x")
    print()

    return results


def benchmark_deserialization(payload: Dict[str, Any], iterations: int = 1000):
    """Benchmark deserialization speed"""

    print(f"🔓 Benchmarking deserialization ({iterations} iterations)")
    print("=" * 60)

    # Prepare serialized data
    json_bytes = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    json_compressed = zlib.compress(json_bytes, level=6)
    msgpack_bytes = msgpack.packb(payload, use_bin_type=True)
    msgpack_compressed = zlib.compress(msgpack_bytes, level=6)

    # Test JSON + zlib deserialization
    print("1️⃣  JSON + zlib decompression")
    start_time = time.time()
    for _ in range(iterations):
        decompressed = zlib.decompress(json_compressed)
        json.loads(decompressed.decode("utf-8"))
    json_deserial_time = time.time() - start_time
    print(f"   ⏱️  Time: {json_deserial_time:.4f}s")
    print()

    # Test MessagePack deserialization
    print("2️⃣  MessagePack (no compression)")
    start_time = time.time()
    for _ in range(iterations):
        msgpack.unpackb(msgpack_bytes, raw=False)
    msgpack_deserial_time = time.time() - start_time
    print(f"   ⏱️  Time: {msgpack_deserial_time:.4f}s")
    print(
        f"   📊 vs JSON+zlib: {json_deserial_time / msgpack_deserial_time:.2f}x faster"
    )
    print()

    # Test MessagePack + zlib deserialization
    print("3️⃣  MessagePack + zlib decompression")
    start_time = time.time()
    for _ in range(iterations):
        decompressed = zlib.decompress(msgpack_compressed)
        msgpack.unpackb(decompressed, raw=False)
    msgpack_zlib_deserial_time = time.time() - start_time
    print(f"   ⏱️  Time: {msgpack_zlib_deserial_time:.4f}s")
    print(
        f"   📊 vs JSON+zlib: {json_deserial_time / msgpack_zlib_deserial_time:.2f}x faster"
    )
    print()

    return {
        "json_zlib": json_deserial_time,
        "msgpack": msgpack_deserial_time,
        "msgpack_zlib": msgpack_zlib_deserial_time,
    }


def analyze_hash_consistency():
    """Test if switching to MessagePack affects SHA-256 hash consistency"""

    print("🔐 Testing SHA-256 hash consistency")
    print("=" * 60)

    payload = create_sample_pncp_payload()

    # Current approach: hash of JSON bytes
    json_bytes = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    json_hash = hashlib.sha256(json_bytes).hexdigest()

    # MessagePack approach: hash of MessagePack bytes
    msgpack_bytes = msgpack.packb(payload, use_bin_type=True)
    msgpack_hash = hashlib.sha256(msgpack_bytes).hexdigest()

    print(f"JSON SHA-256:     {json_hash}")
    print(f"MessagePack SHA-256: {msgpack_hash}")
    print(
        f"Hashes match: {'✅ Yes' if json_hash == msgpack_hash else '❌ No (expected)'}"
    )
    print()

    # Test consistency across multiple serializations
    print("Testing serialization consistency:")
    hashes_json = set()
    hashes_msgpack = set()

    for i in range(10):
        json_bytes = json.dumps(
            payload, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        msgpack_bytes = msgpack.packb(payload, use_bin_type=True)

        hashes_json.add(hashlib.sha256(json_bytes).hexdigest())
        hashes_msgpack.add(hashlib.sha256(msgpack_bytes).hexdigest())

    print(
        f"JSON hash consistency: {'✅ Consistent' if len(hashes_json) == 1 else '❌ Inconsistent'}"
    )
    print(
        f"MessagePack hash consistency: {'✅ Consistent' if len(hashes_msgpack) == 1 else '❌ Inconsistent'}"
    )
    print()


def calculate_storage_savings(current_size_mb: float, compression_ratio: float):
    """Calculate potential storage savings"""

    print("💾 Storage Impact Analysis")
    print("=" * 60)

    new_size_mb = current_size_mb / compression_ratio
    savings_mb = current_size_mb - new_size_mb
    savings_percent = (savings_mb / current_size_mb) * 100

    print(f"Current storage (estimated): {current_size_mb:,.1f} MB")
    print(f"With MessagePack: {new_size_mb:,.1f} MB")
    print(f"Savings: {savings_mb:,.1f} MB ({savings_percent:.1f}%)")
    print()

    # Estimate cost savings (rough AWS S3 pricing)
    cost_per_gb_month = 0.023  # ~$0.023 per GB/month for S3 Standard
    current_cost_month = (current_size_mb / 1024) * cost_per_gb_month
    new_cost_month = (new_size_mb / 1024) * cost_per_gb_month
    monthly_savings = current_cost_month - new_cost_month

    print(f"Estimated monthly storage cost savings: ${monthly_savings:.2f}")
    print(f"Estimated annual storage cost savings: ${monthly_savings * 12:.2f}")
    print()


def main():
    """Run comprehensive MessagePack analysis"""

    print("🔬 MSGPACK vs JSON+ZLIB ANALYSIS FOR BALIZA")
    print("=" * 60)
    print()

    # Create realistic payload
    payload = create_sample_pncp_payload()

    # Run benchmarks
    serialization_results = benchmark_serialization_methods(payload, iterations=1000)
    benchmark_deserialization(payload, iterations=1000)

    # Test hash consistency
    analyze_hash_consistency()

    # Calculate storage impact
    current_size_mb = 1000  # Estimate 1GB current storage
    best_compression_ratio = (
        serialization_results["json_zlib"]["avg_size"]
        / serialization_results["msgpack_zlib"]["avg_size"]
    )
    calculate_storage_savings(current_size_mb, best_compression_ratio)

    # Summary recommendations
    print("📋 RECOMMENDATIONS")
    print("=" * 60)

    json_zlib_size = serialization_results["json_zlib"]["avg_size"]
    msgpack_zlib_size = serialization_results["msgpack_zlib"]["avg_size"]
    size_improvement = json_zlib_size / msgpack_zlib_size

    json_zlib_time = serialization_results["json_zlib"]["serialization_time"]
    msgpack_zlib_time = serialization_results["msgpack_zlib"]["serialization_time"]

    if (
        msgpack_zlib_size < json_zlib_size * 0.9
        and msgpack_zlib_time <= json_zlib_time * 1.2
    ):
        print("✅ RECOMMENDED: Switch to MessagePack + zlib")
        print(f"   📦 Size reduction: {size_improvement:.1f}x smaller")
        print("   ⚡ Performance impact: Acceptable")
        print("   🔄 Migration effort: Medium (need to handle existing data)")
    elif msgpack_zlib_size < json_zlib_size * 0.95:
        print("⚠️  CONDITIONAL: Consider MessagePack + zlib")
        print(f"   📦 Size reduction: {size_improvement:.1f}x smaller")
        print("   ⚡ Performance: Need to evaluate in production")
    else:
        print("❌ NOT RECOMMENDED: Stick with current JSON + zlib")
        print("   📦 Size difference: Not significant enough")
        print("   ⚡ Migration cost: Not justified")

    print()
    print("🚨 IMPORTANT CONSIDERATIONS:")
    print("- Hash consistency: Would break existing SHA-256 hashes")
    print("- Migration complexity: Need strategy for existing compressed data")
    print("- Debugging: MessagePack is less human-readable than JSON")
    print("- Dependencies: Adds msgpack-python dependency")
    print("- Compatibility: May affect integration with other tools")


if __name__ == "__main__":
    try:
        import msgpack
    except ImportError:
        print("❌ msgpack not installed. Install with: pip install msgpack")
        sys.exit(1)

    main()
