import os

def atomic_write(filename: str, content: str):
    """
    Writes atomically the given content to the given file.
    
    The write is atomic since it writes first to a temporary file, and
    if the write succeeds, it (atomically) renames the temporary file
    to the given filename. Before renaming the file, it waits for the
    contents to be flushed to the hard drive.
    """
    temp_filename = f"{filename}.temp"
    try:
        with open(temp_filename, "w", encoding="utf-8") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_filename, filename)
    except Exception as e:
        print(f"Failed to write atomically. Error: {e}")
