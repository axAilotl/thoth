"""Bounded AI-export readers. Historical text is data, never job authority.

Only conversation text is kept. Tool calls, tool results, reasoning traces,
images, attachments and other non-text parts are dropped and counted, so one
multimodal message never rejects the whole conversation. A message with no text
left is skipped and counted as ``empty_message``.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json

PROVIDERS = frozenset({"chatgpt", "claude", "grok", "normalized"})
ROLES = frozenset({"user", "assistant", "system", "developer", "tool"})

# ChatGPT content types that are text the person or assistant wrote.
_CHATGPT_TEXT = frozenset({"text", "multimodal_text"})
# ChatGPT content types that are tool calls, tool output or reasoning.
_CHATGPT_STRIPPED = {
    "code": "tool_use", "execution_output": "tool_result", "tether_browsing_display": "tool_result",
    "tether_quote": "tool_result", "system_error": "tool_result", "computer_output": "tool_result",
    "thoughts": "thinking", "reasoning_recap": "thinking", "user_editable_context": "other",
}
# Claude content block types and the category they count under.
_CLAUDE_STRIPPED = {
    "tool_use": "tool_use", "server_tool_use": "tool_use", "tool_result": "tool_result",
    "web_search_tool_result": "tool_result", "thinking": "thinking", "redacted_thinking": "thinking",
    "image": "image", "document": "attachment",
}
# Grok response fields that are not conversation text.
_GROK_STRIPPED = {
    "thinking_trace": "thinking", "steps": "tool_use", "web_search_results": "tool_result",
    "file_attachments": "attachment", "card_attachments_json": "attachment",
    "generated_image_urls": "image", "xpost_ids": "attachment",
}


class AIExportParser:
    def __init__(self, *, max_export_bytes, max_messages, max_message_characters):
        self.max_export_bytes = max_export_bytes
        self.max_messages = max_messages
        self.max_message_characters = max_message_characters

    def parse(self, content: str, *, provider: str, source_id: str, project_id: str):
        if not isinstance(content, str) or len(content.encode()) > self.max_export_bytes:
            raise ValueError("history_export_budget_exceeded")
        if provider not in PROVIDERS:
            raise ValueError("unsupported_history_provider")
        document = json.loads(content)
        stripped: Counter = Counter()
        if provider == "grok":
            session, messages = self._grok(document, source_id, stripped)
        else:
            sessions = document if isinstance(document, list) else [document]
            matches = [item for item in sessions if isinstance(item, dict) and
                       str(item.get("id") or item.get("uuid")) == source_id]
            if len(matches) != 1:
                raise ValueError("selected_history_session_missing_or_ambiguous")
            session = matches[0]
            if session.get("project_id") not in (None, project_id):
                raise ValueError("history_project_mismatch")
            if provider == "chatgpt":
                raw = self._chatgpt_branch(session)
            else:
                raw = session.get("chat_messages" if provider == "claude" else "messages")
                if not isinstance(raw, list):
                    raise ValueError("invalid_history_messages")
            if len(raw) > self.max_messages:
                raise ValueError("history_message_budget_exceeded")
            messages = [self._message(message, provider, stripped) for message in raw]
        messages = [message for message in messages if message is not None]
        if not messages:
            raise ValueError("history_no_text_content")
        if len(messages) > self.max_messages:
            raise ValueError("history_message_budget_exceeded")
        return {"source_id": source_id, "project_id": project_id, "provider": provider,
                "title": str(session.get("title") or session.get("name") or source_id),
                "created_at": _timestamp(session.get("create_time", session.get("created_at"))),
                "updated_at": _timestamp(session.get("update_time", session.get("modify_time", session.get("updated_at")))),
                "messages": messages, "stripped_parts": dict(sorted(stripped.items()))}

    def _chatgpt_branch(self, session):
        mapping = session.get("mapping")
        current = session.get("current_node")
        if not isinstance(mapping, dict) or not current:
            raise ValueError("history_current_branch_required")
        visited, messages = set(), []
        while current is not None:
            if current in visited or current not in mapping:
                raise ValueError("invalid_history_branch")
            visited.add(current)
            node = mapping[current]
            if node.get("message") is not None:
                messages.append(node["message"])
                if len(messages) > self.max_messages:
                    raise ValueError("history_message_budget_exceeded")
            current = node.get("parent")
        return list(reversed(messages))

    def _grok(self, document, source_id, stripped):
        """Grok ``prod-grok-backend.json``: keep the current branch ending at the leaf."""
        conversations = document.get("conversations") if isinstance(document, dict) else None
        if not isinstance(conversations, list):
            raise ValueError("invalid_history_messages")
        matches = [item for item in conversations if isinstance(item, dict) and
                   isinstance(item.get("conversation"), dict) and str(item["conversation"].get("id")) == source_id]
        if len(matches) != 1:
            raise ValueError("selected_history_session_missing_or_ambiguous")
        session, responses = matches[0]["conversation"], matches[0].get("responses")
        if not isinstance(responses, list):
            raise ValueError("invalid_history_messages")
        nodes = {}
        for item in responses:
            response = item.get("response") if isinstance(item, dict) else None
            if not isinstance(response, dict) or not response.get("_id"):
                raise ValueError("invalid_history_message")
            nodes[str(response["_id"])] = response
        # Grok project/workspace fields name Grok's own folders, not the Cissa
        # project; the selected Cissa source grant is the project authority.
        # Real exports link assistant answers to their prompt reliably, but most
        # human messages name a parent outside the export. Walk parents from
        # the leaf; where the link leaves the export, continue with the latest
        # earlier response (regenerated answers come later than abandoned ones).
        def order(key):
            response = nodes[key]
            return (_milliseconds(response.get("create_time")),
                    0 if str(response.get("sender")).lower() == "human" else 1, key)
        ordered = sorted(nodes, key=order)
        current = session.get("leaf_response_id")
        if current not in nodes:
            current = ordered[-1] if ordered else None
        visited, branch = set(), []
        while current is not None:
            if current in visited:
                raise ValueError("invalid_history_branch")
            visited.add(current)
            branch.append(nodes[current])
            if len(branch) > self.max_messages:
                raise ValueError("history_message_budget_exceeded")
            parent = nodes[current].get("parent_response_id")
            if parent in nodes:
                current = parent
                continue
            earlier = [key for key in ordered if order(key) < order(current) and key not in visited]
            current = earlier[-1] if earlier else None
        return session, [self._message(response, "grok", stripped) for response in reversed(branch)]

    def _message(self, message, provider, stripped):
        if not isinstance(message, dict):
            raise ValueError("invalid_history_message")
        role = {"chatgpt": lambda: (message.get("author") or {}).get("role"),
                "claude": lambda: message.get("sender"),
                "grok": lambda: str(message.get("sender") or "").lower()}.get(provider, lambda: message.get("role"))()
        role = {"human": "user"}.get(role, role)
        if role not in ROLES:
            raise ValueError("invalid_history_role")
        if provider == "chatgpt":
            text = _chatgpt_text(message, role, stripped)
        elif provider == "claude":
            text = _claude_text(message, stripped)
        elif provider == "grok":
            text = message.get("message")
            for key, category in _GROK_STRIPPED.items():
                if message.get(key):
                    stripped[category] += 1
        else:
            text = message.get("text")
        if text is None:
            text = ""
        if not isinstance(text, str):
            raise ValueError("invalid_history_message")
        if not text.strip():
            stripped["empty_message"] += 1
            return None
        if len(text) > self.max_message_characters:
            raise ValueError("history_message_budget_exceeded")
        model = message.get("model") or (message.get("metadata") or {}).get("model_slug")
        return {"id": str(message.get("id") or message.get("uuid") or message.get("_id") or ""), "role": role, "text": text,
                "created_at": _timestamp(message.get("create_time", message.get("created_at"))),
                "model": model if isinstance(model, str) else None}


def _chatgpt_text(message, role, stripped):
    content = message.get("content") or {}
    if not isinstance(content, dict):
        raise ValueError("invalid_history_message")
    content_type = content.get("content_type", "text")
    if role == "tool":
        stripped["tool_result"] += 1
        return ""
    if content_type not in _CHATGPT_TEXT:
        stripped[_CHATGPT_STRIPPED.get(content_type, "other")] += 1
        return ""
    parts = content.get("parts") or []
    if not isinstance(parts, list):
        raise ValueError("invalid_history_message")
    texts = []
    for part in parts:
        if isinstance(part, str):
            texts.append(part)
        elif isinstance(part, dict) and "asset_pointer" in str(part.get("content_type", "")):
            stripped["image" if "image" in str(part.get("content_type")) else "attachment"] += 1
        else:
            stripped["other"] += 1
    attachments = (message.get("metadata") or {}).get("attachments")
    if isinstance(attachments, list) and attachments:
        stripped["attachment"] += len(attachments)
    return "\n".join(text for text in texts if text)


def _claude_text(message, stripped):
    for key in ("attachments", "files"):
        if isinstance(message.get(key), list) and message[key]:
            stripped["attachment"] += len(message[key])
    blocks = message.get("content")
    if not isinstance(blocks, list) or not blocks:
        return message.get("text")
    texts = []
    for block in blocks:
        kind = block.get("type") if isinstance(block, dict) else None
        if kind == "text" and isinstance(block.get("text"), str):
            texts.append(block["text"])
        else:
            stripped[_CLAUDE_STRIPPED.get(kind, "other")] += 1
    return "\n".join(text for text in texts if text)


def _milliseconds(value):
    stamp = _timestamp(value)
    return datetime.fromisoformat(stamp).timestamp() if stamp else 0.0


def _timestamp(value):
    """UTC ISO time from epoch seconds, ISO text or Mongo extended JSON ``$date``."""
    if value is None:
        return None
    if isinstance(value, dict):
        if set(value) != {"$date"}:
            raise ValueError("invalid_history_timestamp")
        value = value["$date"]
        if isinstance(value, dict):
            if set(value) != {"$numberLong"}:
                raise ValueError("invalid_history_timestamp")
            try:
                return datetime.fromtimestamp(int(value["$numberLong"]) / 1000, timezone.utc).isoformat()
            except (TypeError, ValueError):
                raise ValueError("invalid_history_timestamp") from None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return datetime.fromtimestamp(value, timezone.utc).isoformat()
    if not isinstance(value, str):
        raise ValueError("invalid_history_timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("invalid_history_timestamp")
    return parsed.astimezone(timezone.utc).isoformat()
