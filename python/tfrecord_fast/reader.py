from tfrecord_fast._lowlevel import NumpyTfRecordReader
from typing import Optional, List


# class NumpyTfRecordReader:
#     def __init__(
#         self,
#         path: str,
#         check_integrity: bool = False,
#         shuffle_buffer_size: int = 0,
#         reader_buffer_size: Optional[int] = None,
#         context_keys: Optional[List[str]] = None,
#         sequence_keys: Optional[List[str]] = None,
#         shuffle_seed: Optional[int] = None,
#     ) -> None:
#         self._reader = _NumpyTfRecordReader(
#             path,
#             check_integrity=check_integrity,
#             shuffle_buffer_size=shuffle_buffer_size,
#             reader_buffer_size=reader_buffer_size,
#             context_keys=context_keys,
#             sequence_keys=sequence_keys,
#             shuffle_seed=shuffle_seed,
#         )


#     def __iter__(self):
#         return self
    

#     def __next__(self):
#         while True:

    

    

