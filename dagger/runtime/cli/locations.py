"""
Retrieve from / store into the specified locations.

At the moment, only locations in the local filesystem are supported.
"""


def retrieve_input_from_location(input_location: str) -> bytes:
    """
    Given an input location, retrieve the contents of the file it points to.

    Parameters
    ----------
    input_location
        A pointer to a file (e.g. "/my/filesystem/file.txt")


    Returns
    -------
    The contents of the file.


    Raises
    ------
    FileNotFoundError
        If the file cannot be located.

    PermissionError
        If the current execution context doesn't have enough permissions to read the file.
    """
    with open(input_location, "rb") as f:
        return f.read()


def store_output_in_location(serialized_output: bytes, output_location: str):
    """
    Store a serialized output into the specified location.

    Parameters
    ----------
    serialized_output
        A sequence of bytes representing the output to store

    output_location
        A pointer to a file (e.g. "/my/filesystem/file.txt")


    Raises
    ------
    FileNotFoundError
        If the file cannot be located.

    PermissionError
        If the current execution context doesn't have enough permissions to read the file.
    """
    with open(output_location, "wb") as f:
        f.write(serialized_output)
