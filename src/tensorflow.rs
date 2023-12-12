include!("proto/tensorflow.rs");

use numpy::PyArray1;
use pyo3::{prelude::*, types::PyList};

impl Example {}

pub fn float_list_to_pyarray<'a>(py: Python<'a>, float_list: FloatList) -> &'a PyArray1<f32> {
    PyArray1::from_vec(py, float_list.value)
}

pub fn int64_list_to_pyarray<'a>(py: Python<'a>, int64_list: Int64List) -> &'a PyArray1<i64> {
    PyArray1::from_vec(py, int64_list.value)
}

pub fn bytes_list_to_pyarray<'a>(py: Python<'a>, bytes_list: BytesList) -> &'a PyList {
    bytes_list
        .value
        .into_iter()
        .fold(PyList::empty(py), |list, buf| {
            list.append(PyArray1::from_vec(py, buf))
                .expect("fail to append");
            list
        })
}
