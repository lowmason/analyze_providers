"""Employment-to-size-class assignment."""

import polars as pl

# Labels for QCEW-style size classes: 1-4, 5-9, 10-19, 20-49, 50-99, 100-249, 250-499, 500+
SIZE_CLASS_LABELS: list[str] = [
    '1-4',
    '5-9',
    '10-19',
    '20-49',
    '50-99',
    '100-249',
    '250-499',
    '500+',
]


# Upper bounds for each size class bin: 1-4, 5-9, 10-19, 20-49, 50-99, 100-249, 250-499, 500+
_SIZE_CLASS_UPPERS: list[int] = [4, 9, 19, 49, 99, 249, 499]


def assign_size_class(employment: int) -> str:
    """Assign a size class label from employment count.

    Args:
        employment: Number of employees.

    Returns:
        Label such as '1-4', '5-9', ..., '500+'.
    """
    if employment <= 0:
        return '1-4'
    for i, upper in enumerate(_SIZE_CLASS_UPPERS):
        if employment <= upper:
            return SIZE_CLASS_LABELS[i]
    return SIZE_CLASS_LABELS[-1]


def size_class_expr(col: str) -> pl.Expr:
    """Polars expression for size class from an employment column.

    Uses pl.when().then() chains for use inside with_columns.

    Args:
        col: Name of the column containing employment (integer).

    Returns:
        Expression that evaluates to size class string.
    """
    e = pl.col(col)
    return (
        pl.when(e <= 0)
        .then(pl.lit('1-4'))
        .when(e <= 4)
        .then(pl.lit('1-4'))
        .when(e <= 9)
        .then(pl.lit('5-9'))
        .when(e <= 19)
        .then(pl.lit('10-19'))
        .when(e <= 49)
        .then(pl.lit('20-49'))
        .when(e <= 99)
        .then(pl.lit('50-99'))
        .when(e <= 249)
        .then(pl.lit('100-249'))
        .when(e <= 499)
        .then(pl.lit('250-499'))
        .otherwise(pl.lit('500+'))
    )
