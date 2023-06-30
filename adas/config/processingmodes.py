"""This module containing properties for allowed processing modes."""

from typing import List

STREAM = "stream"
BATCH = "batch"


def valid_modes() -> List[str]:
    """Returns valid processing modes as a list of strings."""
    valid_processing_modes = [STREAM, BATCH]
    return valid_processing_modes
