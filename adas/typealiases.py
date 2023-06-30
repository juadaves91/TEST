"""This module contains type aliases used throughout the project."""

from typing import List, Union

from adas.config import processingmodes, writemodes

# Type aliases
ProcessMode = Union[processingmodes.BATCH, processingmodes.STREAM]
WriteMode = Union[writemodes.MERGE, writemodes.APPEND]
Date = Union[str, None]
Hours = Union[int, List[int], None]
