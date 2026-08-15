from pathlib import Path


def test_comparar_does_not_present_simulated_peers_as_real_municipalities() -> None:
    source = Path("web/src/components/CompararView.svelte").read_text(encoding="utf-8")

    assert "Referências simuladas" in source
    assert "Não representam municípios reais" in source
    assert "Municípios Semelhantes" not in source
    assert "Peer 1 (Simulado)" not in source
    assert "Cenário −10%" in source
