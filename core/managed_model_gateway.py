"""Explicit Cissa model authority; no provider SDK, subprocess or env lookup."""
from __future__ import annotations
from copy import deepcopy
from types import SimpleNamespace


class ManagedModelGateway:
    def __init__(self, *, identity, tasks, invoke):
        self.identity, self.tasks, self.invoke = dict(identity), deepcopy(tasks), invoke
        for task, route in self.tasks.items():
            if not route.get("model"):
                raise ValueError(f"Managed model route {task} requires model")
            if type(route.get("max_input_characters")) is not int or route["max_input_characters"] < 1:
                raise ValueError(f"Managed model route {task} requires input budget")
            if task != "embedding" and (type(route.get("max_tokens")) is not int or route["max_tokens"] < 1):
                raise ValueError(f"Managed model route {task} requires token budget")

    def resolve_task_route(self, task):
        route = self.tasks.get(task)
        return ("cissa", route["model"], dict(route)) if route else None

    async def generate(self, prompt, system_prompt=None, *, task, provider, model, **kwargs):
        route = self.tasks.get(task)
        if route is None or provider != "cissa" or model != route["model"]:
            raise ValueError("Unapproved managed model route")
        max_tokens = kwargs.get("max_tokens")
        if type(max_tokens) is not int or not 0 < max_tokens <= route["max_tokens"]:
            raise ValueError("Managed output token budget exceeded")
        if len(prompt) + len(system_prompt or "") > route["max_input_characters"]:
            raise ValueError("Managed input budget exceeded")
        result = await self.invoke({**self.identity, "task": task, "model": model,
            "prompt": prompt, "system_prompt": system_prompt,
            "max_tokens": max_tokens, "temperature": kwargs.get("temperature")})
        return SimpleNamespace(content=result.get("content", ""), error=result.get("error"))

    async def embed_texts(self, texts, *, task="embedding", **kwargs):
        route = self.tasks.get(task)
        if route is None:
            raise ValueError("No managed embedding authority")
        if sum(len(text) for text in texts) > route["max_input_characters"]:
            raise ValueError("Managed embedding input budget exceeded")
        result = await self.invoke({**self.identity, "task": task, "model": route["model"], "texts": texts})
        return SimpleNamespace(vectors=result.get("vectors", []), error=result.get("error"), model=route["model"], provider="cissa")
