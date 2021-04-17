def retrieve_input_from_location(input_location: str) -> bytes:
    with open(input_location, "rb") as f:
        return f.read()


def store_output_in_location(serialized_output: bytes, output_location: str):
    # TODO: Support extra locations (s3, memory, etc.)
    with open(output_location, "wb") as f:
        f.write(serialized_output)
