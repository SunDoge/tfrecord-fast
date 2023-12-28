use pyo3::{prelude::*, types::PyBytes};
use std::{
    fs::File,
    io::{BufWriter, Write},
};

use crate::utils::crc32c::get_masked_crc;

#[pyclass]
pub struct RustTfRecordWriter {
    file: BufWriter<File>,
}

#[pymethods]
impl RustTfRecordWriter {
    #[new]
    pub fn new(path: &str, buffer_size: usize) -> anyhow::Result<Self> {
        let file = File::create(path)?;
        let file = BufWriter::with_capacity(buffer_size, file);
        Ok(Self { file })
    }

    pub fn write(&mut self, py_bytes: &PyBytes) -> anyhow::Result<()> {
        let buf = py_bytes.as_bytes();
        let length = buf.len() as u64;
        let length_bytes = length.to_le_bytes();

        let length_crc32 = get_masked_crc(&length_bytes);
        let buf_crc32 = get_masked_crc(buf);

        self.file.write(&length_bytes)?;
        self.file.write(&length_crc32.to_le_bytes())?;
        self.file.write(buf)?;
        self.file.write(&buf_crc32.to_le_bytes())?;

        Ok(())
    }

    pub fn write_v2(&mut self, py_bytes: &PyBytes) -> anyhow::Result<()> {
        std::thread::scope(|s| {
            let buf = py_bytes.as_bytes();

            let handle = s.spawn(|| get_masked_crc(buf));

            let length = buf.len() as u64;
            let length_bytes = length.to_le_bytes();
            let length_crc32 = get_masked_crc(&length_bytes);

            self.file.write(&length_bytes)?;
            self.file.write(&length_crc32.to_le_bytes())?;
            self.file.write(buf)?;
            self.file.write(
                &handle
                    .join()
                    .expect("fail to compute crc32 for buf")
                    .to_le_bytes(),
            )?;
            Ok(())
        })
    }
}
