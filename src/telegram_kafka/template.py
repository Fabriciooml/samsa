"""Small, non-executable JSON-field template language for MarkdownV2 messages."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any


class InvalidTemplate(ValueError):
    pass


_FIELD = re.compile(r"[A-Za-z_][A-Za-z_0-9]*(?:\.[A-Za-z_][A-Za-z_0-9]*)*")
_RESERVED = set(r"\_*[]()~`>#+-=|{}.!")


def _segments(template: str) -> list[tuple[str, bool]]:
    segments: list[tuple[str, bool]] = []
    pos = 0
    in_code = False
    in_link_target = False
    while pos < len(template):
        if template.startswith("{{", pos):
            end = template.find("}}", pos + 2)
            if end < 0:
                raise InvalidTemplate("Unclosed placeholder")
            name = template[pos + 2 : end].strip()
            if not _FIELD.fullmatch(name):
                raise InvalidTemplate(f"Invalid field path: {name}")
            if in_code or in_link_target:
                raise InvalidTemplate("Placeholder in code or link target")
            segments.append((name, True))
            pos = end + 2
            continue
        if template.startswith("}}", pos):
            raise InvalidTemplate("Unexpected closing braces")
        char = template[pos]
        escaped = pos > 0 and template[pos - 1] == "\\"
        if char == "`" and not escaped:
            in_code = not in_code
        elif char == "(" and pos > 0 and template[pos - 1] == "]" and not escaped:
            in_link_target = True
        elif char == ")" and in_link_target and not escaped:
            in_link_target = False
        segments.append((char, False))
        pos += 1
    if in_code or in_link_target:
        raise InvalidTemplate("Unclosed Markdown code or link target")
    return segments


def validate_template(template: str) -> None:
    if not template or not template.strip():
        raise InvalidTemplate("Template is empty")
    _segments(template)


def _lookup(message: Mapping[str, Any], path: str) -> Any:
    current: Any = message
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            raise InvalidTemplate(f"Missing field: {path}")
        current = current[part]
    if current is None:
        raise InvalidTemplate(f"Null field: {path}")
    return current


def _literal(value: Any) -> tuple[str, int]:
    if isinstance(value, str):
        raw = value
    else:
        raw = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return "".join("\\" + char if char in _RESERVED else char for char in raw), len(raw)


def render_template(template: str, message: Mapping[str, Any]) -> str:
    parts: list[str] = []
    visible_length = 0
    for part, is_field in _segments(template):
        if is_field:
            escaped, length = _literal(_lookup(message, part))
            parts.append(escaped)
            visible_length += length
        else:
            parts.append(part)
            # Static Markdown may contain formatting markers. Counting those
            # keeps this check conservative without parsing Telegram entities.
            visible_length += len(part)
    result = "".join(parts)
    if not result.strip():
        raise InvalidTemplate("Rendered message is empty")
    if visible_length > 4096:
        raise InvalidTemplate("Rendered message exceeds 4096 characters")
    return result
