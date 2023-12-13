from tfrecord_fast._lowlevel import NumpyTfRecordReader

reader = NumpyTfRecordReader(
    "/tmp/data.tfrecord",
    check_integrity=True,
    shuffle_buffer_size=0,
)

print(reader.read_next())
print(reader.read_next())
