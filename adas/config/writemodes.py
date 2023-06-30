"""This module containing properties for allowed data write modes."""

from typing import List

APPEND = "append"
MERGE = "merge"


def valid_modes() -> List[str]:
    """Returns valid write modes as a list of strings."""
    valid_write_modes = [APPEND, MERGE]
    return valid_write_modes
