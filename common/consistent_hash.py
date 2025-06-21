import hashlib


def consistent_hash(input_string):
    """
    Returns the SHA256 hash of input_string.
    """
    encoded_string = input_string.encode("utf-8")
    hasher = hashlib.sha256()
    hasher.update(encoded_string)
    return hasher.hexdigest()
