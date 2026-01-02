"""Utility functions for pygen-spark."""

from __future__ import annotations

# Short-term: Import from private API (same pattern as pygen-spark)
# Long-term: If pygen exports this in __init__.py, we can use:
#   from cognite.pygen import to_snake
from cognite.pygen.utils.text import to_snake


def to_udtf_function_name(view_id: str) -> str:
    """Convert view external_id to UDTF function name using pygen-main's to_snake.
    
    This ensures consistent naming with pygen-main: view_id -> snake_case -> function_name_udtf.
    Uses the same conversion logic as pygen-main, handling edge cases like:
    - "3D" -> "3d" (special handling)
    - "HTTPResponse" -> "http_response"
    - "SmallBoat" -> "small_boat"
    
    Args:
        view_id: View external_id (e.g., "SmallBoat", "Cognite3DModel", "Smallboat")
        
    Returns:
        Function name in snake_case with _udtf suffix (e.g., "small_boat_udtf")
        
    Examples:
        >>> to_udtf_function_name("SmallBoat")
        'small_boat_udtf'
        >>> to_udtf_function_name("Cognite3DModel")
        'cognite_3d_model_udtf'  # Note: pygen-main handles "3D" specially
        >>> to_udtf_function_name("HTTPResponse")
        'http_response_udtf'
        >>> to_udtf_function_name("small_boat_udtf")
        'small_boat_udtf'  # Already in correct format
    """
    # If already ends with _udtf, return as-is
    if view_id.lower().endswith('_udtf'):
        return view_id.lower()
    
    # Use pygen-main's to_snake for consistent conversion
    snake_case = to_snake(view_id)
    return f"{snake_case}_udtf"


