use std::collections::{HashMap, HashSet};
use std::io;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};

use color_eyre::Result;
use pyo3::{pyclass, pymethods, FromPyObject};

use crate::product::Product;

fn get_symbol(row: &str) -> String {
    row.split(" ")
        .skip(5)
        .next()
        .unwrap()
        .to_string()
        .replace("_", "")
}

#[pyclass]
#[derive(Debug, FromPyObject)]
pub struct DownloadArgs {
    #[pyo3(get)]
    date: String,
    #[pyo3(get)]
    ticker: String,
    #[pyo3(get)]
    capacity: usize,
    #[pyo3(get)]
    skip: HashSet<String>,
}

impl DownloadArgs {
    fn iter_rows(&self) -> Result<impl Iterator<Item = io::Result<String>>> {
        let mut cmd = Command::new("twxm")
            .arg(self.date.as_str())
            .arg("opra")
            .arg(format!("{}_*", self.ticker.as_str()))
            .stdout(Stdio::piped())
            .spawn()?;
        let stdout = cmd.stdout.take().unwrap();
        Ok(BufReader::new(stdout).lines())
    }

    fn create_path(&self, base_dir: &str, symbol: &str) -> Result<PathBuf> {
        let path = PathBuf::new()
            .join(base_dir)
            .join(self.date.as_str())
            .join(self.ticker.as_str());
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }
        Ok(path.join(format!("{symbol}.parquet")))
    }

    pub(crate) fn download_impl<I>(&self, base_dir: &str, row_iter: I) -> Result<()>
    where
        I: Iterator<Item = io::Result<String>>,
    {
        let mut products = HashMap::new();
        for row in row_iter {
            let row = row?;
            let symbol = get_symbol(&row);
            if self.skip.contains(&symbol) {
                continue;
            }
            let product = products.entry(symbol.clone()).or_insert(Product::new(
                self.create_path(base_dir, symbol.as_str())?,
                self.capacity,
            ));
            product.push(row.as_str())?;
        }
        Ok(())
    }

    pub(crate) fn download(&self, base_dir: &str) -> Result<()> {
        let row_iter = self.iter_rows()?;
        self.download_impl(base_dir, row_iter)
    }
}

#[pymethods]
impl DownloadArgs {
    #[new]
    pub fn new(date: String, ticker: String, capacity: usize, skip: HashSet<String>) -> Self {
        Self {
            date,
            ticker,
            capacity,
            skip,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{File, OpenOptions};

    use super::*;

    fn test_iter(file_name: &str) -> impl Iterator<Item = io::Result<String>> {
        let path = PathBuf::new().join("test_data").join(file_name);
        let file = OpenOptions::new().read(true).open(path).unwrap();
        BufReader::new(file).lines()
    }

    #[test]
    fn test_get_symbol() -> Result<()> {
        let file = File::open("test_data/spxw.csv")?;
        let mut reader = BufReader::new(file);
        let line = reader.lines().next().unwrap()?;
        assert_eq!(get_symbol(&line), "SPXW220302C04400000");
        Ok(())
    }

    #[test]
    fn test_download() -> Result<()> {
        let iter_fn = || test_iter("spxw.csv");
        let args = DownloadArgs {
            date: "placeholder".to_string(),
            ticker: "placeholder".to_string(),
            capacity: 10000,
            skip: HashSet::new(),
        };
        args.download_impl("test_data", iter_fn())
    }
}
