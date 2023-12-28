mod constants;
mod reader;
pub mod tensorflow;
mod utils;
mod writer;

use pyo3::prelude::*;
use reader::NumpyTfRecordReader;
use writer::RustTfRecordWriter;

/// A Python module implemented in Rust.
#[pymodule]
fn _lowlevel(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<NumpyTfRecordReader>()?;
    m.add_class::<RustTfRecordWriter>()?;
    Ok(())
}
