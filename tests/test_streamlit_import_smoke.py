from __future__ import annotations


def test_streamlit_main_import_smoke() -> None:
    from app.ui.streamlit_app import main

    assert callable(main)
