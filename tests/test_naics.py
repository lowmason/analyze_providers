"""Tests for NAICS mapping utilities."""

import pytest

from analyze_provider.naics import (
    SUPERSECTOR_MAP,
    naics2_to_supersector,
    naics6_to_naics2,
    naics6_to_naics3,
)


def test_naics6_to_naics2() -> None:
    assert naics6_to_naics2('236220') == '23'
    assert naics6_to_naics2('310000') == '31'
    assert naics6_to_naics2('445110') == '44'


def test_naics6_to_naics3() -> None:
    assert naics6_to_naics3('236220') == '236'
    assert naics6_to_naics3('310000') == '310'
    assert naics6_to_naics3('445110') == '445'


def test_naics2_to_supersector() -> None:
    assert naics2_to_supersector('23') == 'Construction'
    assert naics2_to_supersector('31') == 'Manufacturing'
    assert naics2_to_supersector('44') == 'Retail trade'
    assert naics2_to_supersector('99') == 'Other services'


def test_supersector_map_has_expected_keys() -> None:
    assert '23' in SUPERSECTOR_MAP
    assert '31' in SUPERSECTOR_MAP
    assert SUPERSECTOR_MAP['23'] == 'Construction'
