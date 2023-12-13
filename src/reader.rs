use crate::{
    constants::{U32_SIZE, U64_SIZE},
    tensorflow::{
        bytes_list_to_pyarray, feature::Kind, float_list_to_pyarray, int64_list_to_pyarray,
        SequenceExample,
    },
};
use fastrand::Rng;
use prost::Message;
use pyo3::{
    prelude::*,
    types::{PyDict, PyList},
};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Read},
};

#[derive(Debug)]
pub struct TfRecordReader {
    file_reader: BufReader<File>,
    check_integrity: bool,
    length_buffer: [u8; U64_SIZE],
    masked_crc_buffer: [u8; U32_SIZE],
    content_buffer: Vec<u8>,
}

impl TfRecordReader {
    pub fn new(file_reader: BufReader<File>, check_integrity: bool) -> anyhow::Result<Self> {
        Ok(Self {
            file_reader,
            check_integrity,
            length_buffer: [0; U64_SIZE],
            masked_crc_buffer: [0; U32_SIZE],
            content_buffer: Vec::new(),
        })
    }

    pub fn read_content(&mut self) -> anyhow::Result<Option<&[u8]>> {
        match self.file_reader.read_exact(&mut self.length_buffer) {
            Ok(_) => {}
            Err(err) => {
                return match err.kind() {
                    std::io::ErrorKind::UnexpectedEof => Ok(None),
                    _ => Err(err.into()),
                }
            }
        }

        self.file_reader.read_exact(&mut self.masked_crc_buffer)?;

        if self.check_integrity && !self.verify_masked_crc32(&self.length_buffer) {
            panic!("length crc32 mismatch");
        }

        let length = u64::from_le_bytes(self.length_buffer) as usize;
        if length > self.content_buffer.len() {
            self.content_buffer.resize(length * 2, 0);
        }

        self.file_reader
            .read_exact(&mut self.content_buffer[..length])?;
        self.file_reader.read_exact(&mut self.masked_crc_buffer)?;

        if self.check_integrity && !self.verify_masked_crc32(&self.content_buffer[..length]) {
            panic!("content crc32 mismatch");
        }

        Ok(Some(&self.content_buffer[..length]))
    }

    fn verify_masked_crc32(&self, buf: &[u8]) -> bool {
        let expect = u32::from_le_bytes(self.masked_crc_buffer);
        crate::utils::crc32c::verify_masked_crc(buf, expect)
    }
}

#[derive(Debug)]
pub struct MessageDecoder {
    reader: TfRecordReader,
    context_keys: Option<Vec<String>>,
    sequence_keys: Option<Vec<String>>,
}

pub type FeatureMap = HashMap<String, Kind>;
pub type FeatureListMap = HashMap<String, Vec<Kind>>;

impl MessageDecoder {
    pub fn new(
        reader: TfRecordReader,
        context_keys: Option<Vec<String>>,
        sequence_keys: Option<Vec<String>>,
    ) -> Self {
        Self {
            reader,
            context_keys,
            sequence_keys,
        }
    }

    pub fn read_example(&mut self) -> anyhow::Result<Option<SequenceExample>> {
        let example = match self.reader.read_content()? {
            Some(buf) => Some(SequenceExample::decode(buf)?),
            None => None,
        };
        Ok(example)
    }

    pub fn read_by_keys(&mut self) -> Option<(Option<FeatureMap>, Option<FeatureListMap>)> {
        let example = self.read_example().expect("no example");
        match example {
            None => None,
            Some(example) => {
                let feature_map: Option<FeatureMap> = match (example.context, &self.context_keys) {
                    (Some(mut c), Some(keys)) => Some(
                        keys.iter()
                            .map(|k| {
                                c.feature
                                    .remove_entry(k)
                                    .map(|(k, v)| (k, v.kind.expect("no kind")))
                                    .expect("no such entry")
                            })
                            .collect(),
                    ),
                    (Some(c), None) => Some(
                        c.feature
                            .into_iter()
                            .map(|(k, v)| (k, v.kind.expect("no kind")))
                            .collect(),
                    ),
                    (None, Some(_)) => {
                        panic!("no context");
                    }
                    (None, None) => None,
                };

                let feature_list_map: Option<FeatureListMap> =
                    match (example.feature_lists, &self.sequence_keys) {
                        (Some(mut f), Some(keys)) => Some(
                            keys.iter()
                                .map(|k| {
                                    f.feature_list
                                        .remove_entry(k)
                                        .map(|(k, v)| {
                                            (
                                                k,
                                                v.feature
                                                    .into_iter()
                                                    .map(|x| x.kind.expect("no kind"))
                                                    .collect(),
                                            )
                                        })
                                        .expect("no such entry")
                                })
                                .collect(),
                        ),
                        (Some(f), None) => Some(
                            f.feature_list
                                .into_iter()
                                .map(|(k, v)| {
                                    let kinds = v
                                        .feature
                                        .into_iter()
                                        .map(|x| x.kind.expect("no kind"))
                                        .collect();
                                    (k, kinds)
                                })
                                .collect(),
                        ),
                        (None, Some(_)) => {
                            panic!("no context");
                        }
                        (None, None) => None,
                    };
                Some((feature_map, feature_list_map))
            }
        }
    }
}

impl Iterator for &mut MessageDecoder {
    type Item = (Option<FeatureMap>, Option<FeatureListMap>);

    fn next(&mut self) -> Option<Self::Item> {
        self.read_by_keys()
    }
}

#[derive(Debug)]
pub struct Shuffler {
    reader: MessageDecoder,
    buffer: Vec<(Option<FeatureMap>, Option<FeatureListMap>)>,
    buffer_size: usize,
    rng: Rng,
}

impl Shuffler {
    pub fn new(reader: MessageDecoder, buffer_size: usize, seed: Option<u64>) -> Self {
        Self {
            reader,
            buffer: Vec::with_capacity(buffer_size),
            buffer_size,
            rng: seed.map_or_else(Rng::new, Rng::with_seed),
        }
    }

    pub fn fill(&mut self) {
        self.reader.take(self.buffer_size).for_each(|fm| {
            self.buffer.push(fm);
        });
    }

    pub fn read_next(&mut self) -> Option<(Option<FeatureMap>, Option<FeatureListMap>)> {
        if self.buffer_size == 0 {
            return self.reader.read_by_keys();
        }

        if self.buffer.is_empty() {
            self.fill();
        }

        match self.reader.read_by_keys() {
            Some(mut fm) => {
                let index = self.rng.usize(0..self.buffer.len());
                std::mem::swap(&mut fm, &mut self.buffer[index]);
                Some(fm)
            }
            None => {
                if self.buffer.is_empty() {
                    return None;
                }
                let index = self.rng.usize(0..self.buffer.len());
                Some(self.buffer.swap_remove(index))
            }
        }
    }
}

#[pyclass]
pub struct NumpyTfRecordReader {
    reader: Shuffler,
}

#[pymethods]
impl NumpyTfRecordReader {
    #[new]
    pub fn new(
        path: String,
        check_integrity: bool,
        shuffle_buffer_size: usize,
        reader_buffer_size: Option<usize>,
        context_keys: Option<Vec<String>>,
        sequence_keys: Option<Vec<String>>,
        shuffle_seed: Option<u64>,
    ) -> anyhow::Result<Self> {
        let file = File::open(path)?;
        let reader = match reader_buffer_size {
            None => BufReader::new(file),
            Some(size) => BufReader::with_capacity(size, file),
        };
        let reader = TfRecordReader::new(reader, check_integrity)?;
        let reader = MessageDecoder::new(reader, context_keys, sequence_keys);
        let reader = Shuffler::new(reader, shuffle_buffer_size, shuffle_seed);

        Ok(Self { reader })
    }

    pub fn read_next<'a>(
        &mut self,
        py: Python<'a>,
    ) -> Option<(Option<&'a PyDict>, Option<&'a PyDict>)> {
        match self.reader.read_next() {
            None => None,
            Some((feature_map, feature_list_map)) => {
                let feature_dict = feature_map.map(|fm| {
                    fm.into_iter().fold(PyDict::new(py), |dict, (k, v)| {
                        udpate_dict(k, v, py, dict);
                        dict
                    })
                });

                let feature_list_dict = feature_list_map.map(|fm| {
                    fm.into_iter().fold(PyDict::new(py), |dict, (k, v)| {
                        update_list_dict(k, v, py, dict);
                        dict
                    })
                });

                Some((feature_dict, feature_list_dict))
            }
        }
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__<'a>(
        mut slf: PyRefMut<'_, Self>,
        py: Python<'a>,
    ) -> Option<(Option<&'a PyDict>, Option<&'a PyDict>)> {
        slf.read_next(py)
    }
}

pub fn udpate_dict(key: String, value: Kind, py: Python<'_>, dict: &PyDict) {
    match value {
        Kind::FloatList(float_list) => {
            let arr = float_list_to_pyarray(py, float_list);
            dict.set_item(key, arr).expect("fail to add float list");
        }
        Kind::Int64List(int64_list) => {
            let arr = int64_list_to_pyarray(py, int64_list);
            dict.set_item(key, arr).expect("fail to add int64 list");
        }
        Kind::BytesList(bytes_list) => {
            let arrs = bytes_list_to_pyarray(py, bytes_list);
            dict.set_item(key, arrs).expect("fail to add bytes list");
        }
    }
}

pub fn update_list_dict(key: String, value: Vec<Kind>, py: Python<'_>, dict: &PyDict) {
    let arrs = value.into_iter().fold(PyList::empty(py), |list, v| {
        match v {
            Kind::FloatList(float_list) => {
                let arr = float_list_to_pyarray(py, float_list);
                list.append(arr).expect("fail to add float list");
            }
            Kind::Int64List(int64_list) => {
                let arr = int64_list_to_pyarray(py, int64_list);
                list.append(arr).expect("fail to add float list");
            }
            Kind::BytesList(bytes_list) => {
                let arrs = bytes_list_to_pyarray(py, bytes_list);
                list.append(arrs).expect("fail to add float list");
            }
        }
        list
    });
    dict.set_item(key, arrs).expect("fail to set item");
}
