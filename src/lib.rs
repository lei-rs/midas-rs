use color_eyre::Result;
use pyo3::types::PyModule;
use pyo3::{pyfunction, pymodule, wrap_pyfunction, PyResult, Python};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use self::splitter::DownloadArgs;

mod product;
mod splitter;

#[pyfunction]
pub fn download(args: DownloadArgs, base_dir: &str) -> Result<()> {
    args.download(base_dir)
}

#[pyfunction]
pub fn par_download(args: Vec<DownloadArgs>, base_dir: &str, n_workers: usize) -> Result<()> {
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
    Ok(())
}
