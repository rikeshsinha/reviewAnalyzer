from __future__ import annotations

from types import SimpleNamespace

from app.ui.pages import admin


def test_build_refresh_sources_env_passes_selected_platforms() -> None:
    env = admin._build_refresh_sources_env(["reddit", " google_play ", ""])  # noqa: SLF001

    assert env == {"INGESTION_PLATFORMS": "reddit,google_play"}


def test_sync_admin_form_inputs_from_drafts_updates_widget_values_when_requested(monkeypatch) -> None:
    fake_st = SimpleNamespace(
        session_state={
            "admin_sync_widget_values": True,
            "admin_communities_draft": "a\nb",
            "admin_keywords_draft": "k1",
            "admin_web_sites_draft": "example.com",
            "admin_web_keywords_draft": "sleep",
            "admin_web_max_pages_draft": 75,
            "admin_web_min_chars_draft": 600,
            "admin_communities_input": "old",
            "admin_keywords_input": "old",
        }
    )
    monkeypatch.setattr(admin, "st", fake_st)

    admin._sync_admin_form_inputs_from_drafts()  # noqa: SLF001

    assert fake_st.session_state["admin_communities_input"] == "a\nb"
    assert fake_st.session_state["admin_keywords_input"] == "k1"
    assert fake_st.session_state["admin_web_sites_input"] == "example.com"
    assert fake_st.session_state["admin_web_keywords_input"] == "sleep"
    assert fake_st.session_state["admin_web_max_pages_input"] == 75
    assert fake_st.session_state["admin_web_min_chars_input"] == 600
    assert fake_st.session_state["admin_sync_widget_values"] is False


def test_sync_admin_form_inputs_from_drafts_noop_without_flag(monkeypatch) -> None:
    fake_st = SimpleNamespace(
        session_state={
            "admin_sync_widget_values": False,
            "admin_communities_input": "keep",
        }
    )
    monkeypatch.setattr(admin, "st", fake_st)

    admin._sync_admin_form_inputs_from_drafts()  # noqa: SLF001

    assert fake_st.session_state["admin_communities_input"] == "keep"
