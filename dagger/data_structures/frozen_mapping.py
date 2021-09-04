"""Read-only Mapping."""
from collections.abc import Mapping as MappingABC
from typing import Generic, Iterator, Mapping, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class FrozenMapping(Generic[K, V], MappingABC):
    """Implementation of a read-only Mapping."""

    def __init__(
        self,
        mapping: Mapping[K, V],
        error_message: str,
    ):
        """
        Wrap the supplied mapping in an interface that prevents mutations.

        Parameters
        ----------
        mapping
            A map (e.g. a dictionary) of K -> V

        error_message
            Message of the exception generated if someone tries to mutate the dictionary. We use this to provide more helpful error messages to dagger users.

        Returns
        -------
        A FrozenMapping wrapping the supplied mapping.
        """
        self._mapping = mapping
        self._error_message = error_message

    def __getitem__(self, key: K) -> V:
        """
        Get a value in the map given its key.

        Parameters
        ----------
        key : K
            Key to look up

        Returns
        -------
        The value V that corresponds to that key

        Raises
        ------
        KeyError
            If the key is not present in the mapping
        """
        return self._mapping.__getitem__(key)

    def __setitem__(self, key: K, value: V):
        """
        Attempt to set a (k,v) pair. It will always raise an exception.

        Raises
        ------
        TypeError
            Always
        """
        raise TypeError(self._error_message)

    def __delitem__(self, key: K):
        """
        Attempt to delete a key. It will always raise an exception.

        Raises
        ------
        TypeError
            Always
        """
        raise TypeError(self._error_message)

    def __iter__(self) -> Iterator[K]:
        """Get an iterator over the keys of the mapping."""
        return self._mapping.__iter__()

    def __len__(self) -> int:
        """Get the length of the mapping, defined as the number of (key,value) pairs in it."""
        return self._mapping.__len__()

    def __repr__(self) -> str:
        """Get a human-readable string representation of the data structure."""
        return repr(self._mapping)
