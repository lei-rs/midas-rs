use color_eyre::Result;
use pyo3::types::PyModule;
use pyo3::{pyfunction, pymodule, wrap_pyfunction, PyObject, PyRef, PyResult, Python};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};

use self::splitter::DownloadArgs;

mod product;
mod splitter;

#[pyfunction]
pub fn download(args: PyRef<DownloadArgs>, base_dir: &str) -> Result<()> {
    println!("Downloading {:?}", args);
    args.download(base_dir)
}

#[pyfunction]
pub fn par_download(
    py: Python<'_>,
    args: PyObject,
    base_dir: &str,
    n_workers: usize,
) -> Result<()> {
    let args = args.extract::<Vec<DownloadArgs>>(py)?;
    rayon::ThreadPoolBuilder::new()
        .num_threads(n_workers)
        .build_global()?;
    args.into_par_iter()
        .try_for_each(|arg| arg.download(base_dir))?;
    Ok(())
}

#[pymodule]
fn midas_rs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<DownloadArgs>()?;
    m.add_function(wrap_pyfunction!(download, m)?)?;
    m.add_function(wrap_pyfunction!(par_download, m)?)?;
    Ok(())
}
