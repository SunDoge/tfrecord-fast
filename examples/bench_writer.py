from tfrecord_fast import TfRecordWriter
from tqdm import tqdm
import numpy as np
import tfrecord
import struct


class PyTfRecordWriter(tfrecord.TFRecordWriter):
    def write_buf(self, record: bytes):
        length = len(record)
        length_bytes = struct.pack("<Q", length)
        self.file.write(length_bytes)
        self.file.write(self.masked_crc(length_bytes))
        self.file.write(record)
        self.file.write(self.masked_crc(record))


def bench(n: int = 1000):
    x = bytes(1024 * 1024 * 2)
    writer = TfRecordWriter("/tmp/test_writer.tfrec", buffer_size=1024 * 16)
    # writer = PyTfRecordWriter("/tmp/test_writer.tfrec")
    for _ in tqdm(range(n)):
        # writer.write_buf(x)
        # writer.file.flush()

        writer.write(x)
        # writer.flush()


bench()
