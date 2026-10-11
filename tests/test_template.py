import pytest

from telegram_kafka.template import InvalidTemplate, render_template


def test_render_nested_json_value_as_literal_markdown():
    text = render_template("*Order:* {{order.id}}", {"order": {"id": "A_1*"}})
    assert text == "*Order:* A\\_1\\*"


def test_missing_field_fails_instead_of_sending_partial_message():
    with pytest.raises(InvalidTemplate, match="order.id"):
        render_template("Order {{order.id}}", {"order": {}})


def test_placeholder_inside_code_or_link_target_is_rejected():
    for pattern in ("`{{value}}`", "[here]({{value}})"):
        with pytest.raises(InvalidTemplate):
            render_template(pattern, {"value": "x"})


def test_length_limit_counts_inserted_text_after_markdown_escaping():
    assert render_template("{{value}}", {"value": "_" * 4096}) == "\\_" * 4096
    with pytest.raises(InvalidTemplate, match="4096"):
        render_template("{{value}}", {"value": "_" * 4097})
