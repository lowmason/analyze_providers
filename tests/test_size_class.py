"""Tests for size class assignment."""

import polars as pl
import pytest

from analyze_provider.size_class import assign_size_class, size_class_expr


def test_assign_size_class() -> None:
    assert assign_size_class(0) == '1-4'
    assert assign_size_class(1) == '1-4'
    assert assign_size_class(4) == '1-4'
    assert assign_size_class(5) == '5-9'
    assert assign_size_class(9) == '5-9'
    assert assign_size_class(10) == '10-19'
    assert assign_size_class(500) == '500+'
    assert assign_size_class(1000) == '500+'


def test_size_class_expr() -> None:
    df = pl.DataFrame({'emp': [0, 4, 5, 10, 100, 500]})
    out = df.with_columns(size_class_expr('emp').alias('size_class'))
    assert out['size_class'].to_list() == ['1-4', '1-4', '5-9', '10-19', '100-249', '500+']
