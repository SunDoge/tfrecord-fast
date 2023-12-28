from ._lowlevel import RustTfRecordWriter


class TfRecordWriter:
    def __init__(self, path: str, buffer_size: int = 8192) -> None:
        self._writer = RustTfRecordWriter(path, buffer_size)

    def write(self, buf: bytes):
        self._writer.write(buf)

    def write_v2(self, buf: bytes):
        self._writer.write_v2(buf)

    def flush(self):
        self._writer.flush()

    def close(self):
        self._writer.flush()
