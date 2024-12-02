"""
TODO: add file-level docstring
"""

__all__ = ["PyDoughConfigs"]


class PyDoughConfigs:
    """
    Class used to store information about various configuration settings of
    PyDough.
    """

    def __init__(
        self,
        sum_default_zero: bool = True,
        avg_default_zero: bool = False,
        count_default_zero: bool = True,
    ):
        self._sum_default_zero: bool = sum_default_zero
        self._avg_default_zero: bool = avg_default_zero
        self._count_default_zero: bool = count_default_zero

    @property
    def sum_default_zero(self) -> bool:
        """
        Whether to ensure that SUM calculations default to zero if there are no
        records to take the sum of (e.g. all null, or no children).
        """
        return self._sum_default_zero

    def toggle_sum_default_zero(self, flag: bool):
        """
        Switches the value of the `sum_default_zero` config to the specified
        value.
        """
        self._sum_default_zero = flag

    @property
    def avg_default_zero(self) -> bool:
        """
        Whether to ensure that AVG calculations default to zero if there are no
        records to take the average of (e.g. all null, or no children).
        """
        return self._avg_default_zero

    def toggle_avg_default_zero(self, flag: bool):
        """
        Switches the value of the `avg_default_zero` config to the specified
        value.
        """
        self._avg_default_zero = flag

    @property
    def count_default_zero(self) -> bool:
        """
        Whether to ensure that COUNT calculations default to zero if there are no
        records to count of (e.g. all null, or no children).
        """
        return self._count_default_zero

    def toggle_count_default_zero(self, flag: bool):
        """
        Switches the value of the `count_default_zero` config to the specified
        value.
        """
        self._count_default_zero = flag
