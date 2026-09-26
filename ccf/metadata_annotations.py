"""Preserve validated Thoth connector lane declarations as optional CCF ext."""

from collections.abc import Mapping

from core.connector_registry import parse_ccf_extensions, validate_ccf_lane

from .metadata_values import portable


def annotations(registry, source, metadata):
    block = None
    if registry is not None:
        for name in (source.get('collector'), source.get('source_name'), source.get('source_type')):
            if not name:
                continue
            try:
                block = registry.get(str(name)).ccf
                break
            except KeyError:
                continue
    result = block.to_dict() if block is not None else {}
    override = metadata.get('ccf')
    if override is not None:
        origin = 'payload normalized_metadata.ccf'
        if not isinstance(override, Mapping):
            raise ValueError(f'{origin} must be an object')
        unknown = set(override) - {'lane', 'extensions'}
        if unknown:
            raise ValueError(f'{origin} has unknown fields {sorted(unknown)}')
        if override.get('lane') is not None:
            result['lane'] = validate_ccf_lane(override['lane'], origin=origin)
        extensions = parse_ccf_extensions(override.get('extensions'), origin=origin)
        result['extensions'] = {**result.get('extensions', {}), **extensions}
    return {'thoth': portable(result)} if result else {}
