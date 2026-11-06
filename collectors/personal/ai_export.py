"""Bounded AI-export readers. Historical text is data, never job authority."""
from __future__ import annotations

from datetime import datetime, timezone
import json


class AIExportParser:
    def __init__(self, *, max_export_bytes, max_messages, max_message_characters):
        self.max_export_bytes = max_export_bytes
        self.max_messages = max_messages
        self.max_message_characters = max_message_characters

    def parse(self, content: str, *, provider: str, source_id: str, project_id: str):
        if not isinstance(content, str) or len(content.encode()) > self.max_export_bytes:
            raise ValueError("history_export_budget_exceeded")
        document = json.loads(content)
        sessions = document if isinstance(document, list) else [document]
        if provider not in {"chatgpt", "claude", "normalized"}:
            raise ValueError("unsupported_history_provider")
        matches = [item for item in sessions if isinstance(item, dict) and
                   str(item.get("id") or item.get("uuid")) == source_id]
        if len(matches) != 1:
            raise ValueError("selected_history_session_missing_or_ambiguous")
        session = matches[0]
        if session.get("project_id") not in (None, project_id):
            raise ValueError("history_project_mismatch")
        if provider == "chatgpt":
            messages = self._chatgpt(session)
        else:
            messages = session.get("chat_messages" if provider == "claude" else "messages")
            if not isinstance(messages, list):
                raise ValueError("invalid_history_messages")
            messages = [self._message(message, provider) for message in messages]
        if not messages or len(messages) > self.max_messages:
            raise ValueError("history_message_budget_exceeded")
        return {"source_id": source_id, "project_id": project_id, "provider": provider,
                "title": str(session.get("title") or session.get("name") or source_id),
                "created_at": _timestamp(session.get("create_time", session.get("created_at"))),
                "updated_at": _timestamp(session.get("update_time", session.get("updated_at"))),
                "messages": messages}

    def _chatgpt(self, session):
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
            message = node.get("message")
            if message is not None:
                messages.append(self._message(message, "chatgpt"))
                if len(messages) > self.max_messages:
                    raise ValueError("history_message_budget_exceeded")
            current = node.get("parent")
        return list(reversed(messages))

    def _message(self, message, provider):
        if not isinstance(message, dict):
            raise ValueError("invalid_history_message")
        role = message.get("author", {}).get("role") if provider == "chatgpt" else message.get("sender" if provider == "claude" else "role")
        role = {"human": "user"}.get(role, role)
        if role not in {"user", "assistant", "system", "developer", "tool"}:
            raise ValueError("invalid_history_role")
        if provider == "chatgpt":
            parts = message.get("content", {}).get("parts")
            if not isinstance(parts, list) or any(not isinstance(part, str) for part in parts):
                raise ValueError("unsupported_history_content")
            text = "\n".join(parts)
        else:
            text = message.get("text")
            if text is None and provider == "claude":
                blocks = message.get("content", [])
                if not all(isinstance(block, dict) and block.get("type") == "text" for block in blocks):
                    raise ValueError("unsupported_history_content")
                text = "\n".join(block["text"] for block in blocks)
        if not isinstance(text, str) or len(text) > self.max_message_characters:
            raise ValueError("history_message_budget_exceeded")
        return {"id": str(message.get("id") or message.get("uuid") or ""), "role": role, "text": text,
                "created_at": _timestamp(message.get("create_time", message.get("created_at"))),
                "model": message.get("model") or message.get("metadata", {}).get("model_slug")}


def _timestamp(value):
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return datetime.fromtimestamp(value, timezone.utc).isoformat()
    if not isinstance(value, str):
        raise ValueError("invalid_history_timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("invalid_history_timestamp")
    return parsed.astimezone(timezone.utc).isoformat()
