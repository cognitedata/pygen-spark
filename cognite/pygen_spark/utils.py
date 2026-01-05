"""Utility functions for pygen-spark."""

from __future__ import annotations

# Short-term: Import from private API (same pattern as pygen-spark)
# Long-term: If pygen exports this in __init__.py, we can use:
#   from cognite.pygen import to_snake
from cognite.pygen.utils.text import to_snake

try:
    from cognite.client.data_classes.data_modeling.ids import NodeId
    COGNITE_AVAILABLE = True
except ImportError:
    COGNITE_AVAILABLE = False
    # Create dummy class for type checking
    class NodeId:  # type: ignore[no-redef]
        def __init__(self, space: str, external_id: str):
            self.space = space
            self.external_id = external_id


def parse_instance_id(instance_id_str: str) -> NodeId:
    """Parse instance_id string in format 'space:external_id' to NodeId.
    
    This function provides consistent parsing and validation of instance_id strings
    across all time series UDTFs, aligned with pygen-main patterns of working with
    NodeId objects.
    
    Args:
        instance_id_str: Instance ID string in format "space:external_id"
        
    Returns:
        NodeId object
        
    Raises:
        ValueError: If format is invalid or required fields are missing
        
    Examples:
        >>> parse_instance_id("sailboat:ts1")
        NodeId(space='sailboat', external_id='ts1')
        >>> parse_instance_id("space:external:id:with:colons")
        NodeId(space='space', external_id='external:id:with:colons')
    """
    if not instance_id_str:
        raise ValueError("instance_id is required (format: 'space:external_id')")
    
    if ":" not in instance_id_str:
        raise ValueError(f"Invalid instance_id format '{instance_id_str}'. Expected format: 'space:external_id'")
    
    # Split on first colon to handle external_ids that may contain colons
    space, external_id = instance_id_str.split(":", 1)
    space = space.strip()
    external_id = external_id.strip()
    
    if not space or not external_id:
        raise ValueError(f"Invalid instance_id format '{instance_id_str}'. Both space and external_id must be non-empty.")
    
    if not COGNITE_AVAILABLE:
        # Fallback for when cognite-sdk is not available (shouldn't happen in practice)
        return NodeId(space=space, external_id=external_id)
    
    from cognite.client.data_classes.data_modeling.ids import NodeId as CogniteNodeId
    return CogniteNodeId(space=space, external_id=external_id)


def parse_instance_ids(instance_ids_str: str) -> list[NodeId]:
    """Parse comma-separated instance_ids string to list of NodeId.
    
    This function provides consistent parsing and validation of comma-separated
    instance_id strings across all time series UDTFs, aligned with pygen-main
    patterns of working with NodeId objects.
    
    Args:
        instance_ids_str: Comma-separated instance IDs in format "space1:ext_id1,space2:ext_id2"
        
    Returns:
        List of NodeId objects
        
    Raises:
        ValueError: If any format is invalid or required fields are missing
        
    Examples:
        >>> parse_instance_ids("sailboat:ts1,otherspace:ts2")
        [NodeId(space='sailboat', external_id='ts1'), NodeId(space='otherspace', external_id='ts2')]
    """
    if not instance_ids_str:
        raise ValueError("instance_ids is required (format: 'space1:ext_id1,space2:ext_id2')")
    
    node_ids = []
    for instance_id_str in instance_ids_str.split(","):
        instance_id_str = instance_id_str.strip()
        if not instance_id_str:
            continue
        node_ids.append(parse_instance_id(instance_id_str))
    
    if not node_ids:
        raise ValueError("At least one valid instance_id is required")
    
    return node_ids


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


