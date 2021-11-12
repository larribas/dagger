
class EmptyDefaultValue:
    """ Marker object for that represents a parameter without default value. """

    def __repr__(self) -> str:
        """Get a human-readable string representation of an empty default value."""
        return "'None'"

    def __eq__(self, obj):
        """Return true if both inputs are equivalent."""
        return isinstance(obj, EmptyDefaultValue)
