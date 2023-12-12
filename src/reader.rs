use crate::{
    constants::{U32_SIZE, U64_SIZE},
    tensorflow::{
        bytes_list_to_pyarray, feature::Kind, float_list_to_pyarray, int64_list_to_pyarray,
        Example, SequenceExample,
    },
};
use fastrand::Rng;
use prost::Message;
use pyo3::{prelude::*, types::PyDict};
use std::{
    collections::{HashMap, HashSet},
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
    keys: Option<Vec<String>>,
}

pub enum Feature {
    List(Kind),
    Lists(Vec<Kind>),
}
pub type FeatureMap = HashMap<String, Feature>;

impl MessageDecoder {
    pub fn new(reader: TfRecordReader, keys: Option<Vec<String>>) -> Self {
        let keys = keys.map(|keys| (keys, keys.iter().cloned().collect()));
        Self { reader, keys }
    }

    pub fn read_example(&mut self) -> anyhow::Result<Option<SequenceExample>> {
        let example = match self.reader.read_content()? {
            Some(buf) => Some(SequenceExample::decode(buf)?),
            None => None,
        };
        Ok(example)
    }

    pub fn read_by_keys(&mut self) -> Option<FeatureMap> {
        let example = self.read_example().expect("no example");
        match example {
            None => None,
            Some(mut example) => match self.keys {
                None => {
                    let mut feature_map = HashMap::new();
                    example.context.map(|c| {
                        c.feature.into_iter().for_each(|(k, v)| {
                            feature_map.insert(k, Feature::List(v.kind.expect("no kind")));
                        });
                    });
                    example.feature_lists.map(|f| {
                        f.feature_list.into_iter().for_each(|(k, v)| {
                            feature_map.insert(
                                k,
                                Feature::Lists(
                                    v.feature
                                        .into_iter()
                                        .map(|x| x.kind.expect("no kind"))
                                        .collect(),
                                ),
                            );
                        });
                    });
                    Some(feature_map)
                }
                Some(ref keys) => {
                    let mut feature_map = HashMap::new();

                    // let key_set: HashSet<_> = keys.iter().map(|x| x).collect();
                    // let rest_keys: HashSet<_> = example
                    //     .context
                    //     .map(|c| {
                    //         let context_keys: HashSet<&str> =
                    //             c.feature.keys().map(|x| x.as_str()).collect();
                    //         key_set.difference(&context_keys).collect()
                    //     })
                    //     .unwrap_or(key_set);

                    // let context_keys: HashSet<&str> = example.context.map(|c| {
                    //     c.feature.keys().collect()
                    // });

                    // let feature_map = keys
                    //     .iter()
                    //     .map(|key| {
                    //         let (k, v) = example
                    //             .features
                    //             .as_mut()
                    //             .expect("no features")
                    //             .feature
                    //             .remove_entry(key)
                    //             .expect("key not in example");
                    //         (k, v.kind.expect("no kind"))
                    //     })
                    //     .collect();
                    Some(feature_map)
                }
            },
        }
    }
}

impl Iterator for &mut MessageDecoder {
    type Item = FeatureMap;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_by_keys()
    }
}

#[derive(Debug)]
pub struct Shuffler {
    reader: MessageDecoder,
    buffer: Vec<FeatureMap>,
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

    pub fn read_next(&mut self) -> Option<FeatureMap> {
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
        keys: Option<Vec<String>>,
        shuffle_seed: Option<u64>,
    ) -> anyhow::Result<Self> {
        let file = File::open(path)?;
        let reader = match reader_buffer_size {
            None => BufReader::new(file),
            Some(size) => BufReader::with_capacity(size, file),
        };
        let reader = TfRecordReader::new(reader, check_integrity)?;
        let reader = MessageDecoder::new(reader, keys);
        let reader = Shuffler::new(reader, shuffle_buffer_size, shuffle_seed);

        Ok(Self { reader })
    }

    pub fn read_next<'a>(&mut self, py: Python<'a>) -> Option<&'a PyDict> {
        match self.reader.read_next() {
            None => None,
            Some(fm) => {
                let dict = fm.into_iter().fold(PyDict::new(py), |dict, (key, value)| {
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
                    dict
                });
                Some(dict)
            }
        }
    }
}
