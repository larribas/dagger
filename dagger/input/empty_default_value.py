"""Representation of the value of a parameter without default value."""


class EmptyDefaultValue:
    """Marker object that represents a parameter without default value.

    This representation is needed to distinguish no default from a None default value.
    """

    def __repr__(self) -> str:
        """Get a human-readable string representation of an empty default value."""
        return "'None'"

    def __eq__(self, obj):
        """Return true if both inputs are equivalent."""
        return isinstance(obj, EmptyDefaultValue)
