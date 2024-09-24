from gzip import compress, decompress


def decomp(byte_array: bytes) -> str:
    if byte_array is None or len(byte_array) == 0:
        return None
    return decompress(byte_array).decode()


def comp(text: str) -> bytes:
    if text is None or len(text) == 0:
        return None
    return compress(text.encode())
