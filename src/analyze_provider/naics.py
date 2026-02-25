"""NAICS mapping utilities (6-digit to supersector, 3-digit, etc.)."""

import polars as pl

# CES supersector names by 2-digit NAICS. Multiple NAICS codes map to the same supersector.
# Source: BLS CES supersector definitions (e.g. bls.get_mapping('CE', 'supersector')).
SUPERSECTOR_MAP: dict[str, str] = {
    '11': 'Mining and logging',
    '21': 'Mining and logging',
    '22': 'Utilities',
    '23': 'Construction',
    '31': 'Manufacturing',
    '32': 'Manufacturing',
    '33': 'Manufacturing',
    '42': 'Wholesale trade',
    '44': 'Retail trade',
    '45': 'Retail trade',
    '48': 'Transportation and warehousing',
    '49': 'Transportation and warehousing',
    '51': 'Information',
    '52': 'Financial activities',
    '53': 'Financial activities',
    '54': 'Professional and business services',
    '55': 'Professional and business services',
    '56': 'Professional and business services',
    '61': 'Education and health services',
    '62': 'Education and health services',
    '71': 'Leisure and hospitality',
    '72': 'Leisure and hospitality',
    '81': 'Other services',
}


def naics6_to_naics2(code: str) -> str:
    """Return first two digits of a 6-digit NAICS code.

    Args:
        code: 6-digit NAICS code (string, may need zero-padding).

    Returns:
        First two digits as string.
    """
    padded = code.strip().zfill(6)
    return padded[:2]


def naics6_to_naics3(code: str) -> str:
    """Return first three digits of a 6-digit NAICS code.

    Args:
        code: 6-digit NAICS code (string, may need zero-padding).

    Returns:
        First three digits as string.
    """
    padded = code.strip().zfill(6)
    return padded[:3]


def naics2_to_supersector(code: str) -> str:
    """Return BLS CES supersector name for a 2-digit NAICS code.

    Args:
        code: 2-digit NAICS sector code.

    Returns:
        CES supersector name. Unknown codes return 'Other services'.
    """
    key = code.strip().zfill(2) if len(code.strip()) <= 2 else code.strip()[:2]
    return SUPERSECTOR_MAP.get(key, 'Other services')
