"""Preserve opaque CCF annotations while a native writer updates its own fields."""


def preserve_opaque(previous: dict, incoming: dict, owned: dict) -> dict:
    """Remove previously authored keys that disappeared, retain foreign keys.

    Ownership is local state, never added to portable CCF objects. A restored
    object without ownership history is conservatively treated as opaque.
    """
    retained = {}
    for key, value in previous.items():
        if key not in owned:
            retained[key] = value
        elif isinstance(value, dict) and isinstance(owned[key], dict):
            nested = preserve_opaque(value, {}, owned[key])
            if nested:
                retained[key] = nested
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(retained.get(key), dict):
            retained[key] = preserve_opaque(retained[key], value, {})
        else:
            retained[key] = value
    return retained
