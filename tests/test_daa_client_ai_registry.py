from app.daa_client import _ai_strategy_directory_signature, _validate_capabilities


def test_capabilities_preserve_validated_ai_strategy_identity() -> None:
    payload = {
        "contract_version": "pxybacktest.engine-adapter.a-share.v1",
        "worker_version": "daa.a-share-adapter.v1",
        "strategies": [
            {
                "id": "ai_rebound_v1",
                "version": "ai-0123456789ab",
                "source_hash": "a" * 64,
                "entrypoint": "ai_rebound_v1",
                "source": "ai",
                "execution_backend": "matrix_native",
                "registry_status": "validated",
                "engine_types": ["a_share_portfolio"],
            }
        ],
    }

    result = _validate_capabilities(payload)
    strategy = result["strategies"][0]
    assert strategy["source"] == "ai"
    assert strategy["execution_backend"] == "matrix_native"
    assert strategy["registry_status"] == "validated"
    assert strategy["source_hash"] == "a" * 64


def test_ai_strategy_directory_signature_changes_when_strategy_file_changes(tmp_path) -> None:
    ai_dir = tmp_path / "data" / "strategies" / "ai"
    ai_dir.mkdir(parents=True)
    initial = _ai_strategy_directory_signature(ai_dir)
    strategy = ai_dir / "ai_demo.py"
    strategy.write_text("META = {}\n", encoding="utf-8")
    after_create = _ai_strategy_directory_signature(ai_dir)
    assert initial != after_create

    strategy.write_text("META = {'version': '2'}\n", encoding="utf-8")
    after_update = _ai_strategy_directory_signature(ai_dir)
    assert after_update[0] == 1
    assert after_update[1] >= after_create[1]
