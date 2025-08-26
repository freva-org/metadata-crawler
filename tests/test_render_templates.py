import pytest

from metadata_crawler.api.storage_backend import TemplateMixin


def test_basic_string() -> None:
    assert (
        TemplateMixin().render_templates("Hello {{ name }}!", {"name": "Ada"})
        == "Hello Ada!"
    )


def test_nested_containers_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # Prepare environment variables used by ENV / env() / getenv
    monkeypatch.setenv("HOME", "/home/ada")
    monkeypatch.delenv("PORT", raising=False)  # ensure default path is used

    data = {
        "greet": "Hi {{ name }} from {{ ENV.HOME }}",
        "port": "{{ env('PORT', '8000') }}",
        "items": [
            "{{ count }} thing(s)",
            42,
            ("x={{ x }}", "{{ 'HOME'|getenv('/tmp') }}"),
        ],
        "keys": {"k{{ n }}": "v{{ n }}"},
        "set": {"{{ name }}", 1},
    }
    ctx = {"name": "Ada", "count": 3, "x": 7, "n": 1}

    out = TemplateMixin().render_templates(data, ctx)

    assert "Hi Ada" in out["greet"]
    assert out["port"] == "8000"
    assert out["items"][0] == "3 thing(s)"
    assert out["items"][2][0] == "x=7"
    assert out["keys"] == {"k1": "v1"}
    assert out["set"] == {"Ada", 1}


def test_multi_pass_expansion():
    # a -> {{ b }} -> "done" with two passes
    s = "{{ a }}"
    ctx = {"a": "{{ b }}", "b": "done"}

    assert TemplateMixin().render_templates(s, ctx, max_passes=2) == "done"
    assert TemplateMixin().render_templates(s, ctx, max_passes=1) == "{{ b }}"


def test_dict_key_collision_last_wins():
    data = {"a{{ n }}": 1, "a{{ m }}": 2}
    ctx = {"n": 1, "m": 1}
    assert TemplateMixin().render_templates(data, ctx) == {"a1": 2}


def test_non_string_scalars_unchanged():
    assert TemplateMixin().render_templates(123, {}) == 123
    assert TemplateMixin().render_templates(3.14, {}) == 3.14
    assert TemplateMixin().render_templates(True, {}) is True
    assert TemplateMixin().render_templates(None, {}) is None
