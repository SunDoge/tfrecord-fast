use crate::{
    constants::{U32_SIZE, U64_SIZE},
    tensorflow::Example,
};
use prost::Message;
use std::io::Read;

pub struct TfRecordReader<T> {
    file_reader: T,
    check_integrity: bool,
    length_buffer: [u8; U64_SIZE],
    masked_crc_buffer: [u8; U32_SIZE],
    content_buffer: Vec<u8>,
}

impl<T> TfRecordReader<T>
where
    T: Read,
{
    pub fn new(file_reader: T, check_integrity: bool) -> anyhow::Result<Self> {
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

    pub fn read_example(&mut self) -> anyhow::Result<Option<Example>> {
        let example = match self.read_content()? {
            Some(buf) => Some(Example::decode(buf)?),
            None => None,
        };
        Ok(example)
    }

    fn verify_masked_crc32(&self, buf: &[u8]) -> bool {
        let expect = u32::from_le_bytes(self.masked_crc_buffer);
        crate::utils::crc32c::verify_masked_crc(buf, expect)
    }
}

pub struct Shuffler<T> {
    reader: T,
    
}


