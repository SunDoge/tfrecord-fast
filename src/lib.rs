mod constants;
mod reader;
pub mod tensorflow;
mod utils;
mod writer;

use pyo3::{prelude::*, types::PyBytes};
use reader::NumpyTfRecordReader;

/// A Python module implemented in Rust.
#[pymodule]
fn _lowlevel(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<NumpyTfRecordReader>()?;
    Ok(())
}
