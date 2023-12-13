import tfrecord

writer = tfrecord.TFRecordWriter("/tmp/data.tfrecord")
writer.write(
    {"length": (3, "int"), "label": (1, "int")},
    {
        "tokens": ([[0, 0, 1], [0, 1, 0], [1, 0, 0]], "int"),
        "seq_labels": ([0, 1, 1], "int"),
    },
)
writer.write(
    {"length": (3, "int"), "label": (1, "int")},
    {"tokens": ([[0, 0, 1], [1, 0, 0]], "int"), "seq_labels": ([0, 1], "int")},
)
writer.close()
