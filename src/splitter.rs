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
#[derive(FromPyObject)]
pub struct DownloadArgs {
    date: String,
    ticker: String,
    capacity: usize,
    skip: HashSet<String>,
}

impl DownloadArgs {
    fn iter(&self) -> Result<impl Iterator<Item = io::Result<String>>> {
        let mut cmd = Command::new("twxm")
            .arg(self.date.as_str())
            .arg("opra")
            .arg(format!("{}_*", self.ticker.as_str()))
            .stdout(Stdio::piped())
            .spawn()?;
        let stdout = cmd.stdout.take().unwrap();
        Ok(BufReader::new(stdout).lines())
    }

    fn create_path(&self, base_dir: &str) -> Result<PathBuf> {
        let path = PathBuf::new()
            .join(base_dir)
            .join(self.date.as_str())
            .join(self.ticker.as_str());
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }
        Ok(path)
    }

    pub(crate) fn download(&self, base_dir: &str) -> Result<()> {
        let mut products = HashMap::new();
        for row in self.iter()? {
            let row = row?;
            let symbol = get_symbol(&row);
            if self.skip.contains(&symbol) {
                continue;
            }
            let product = products
                .entry(symbol)
                .or_insert(Product::new(self.create_path(base_dir)?, self.capacity));
            product.push(row.as_str())?;
        }
        Ok(())
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
    use std::fs::File;

    use super::*;

    #[test]
    fn test_get_symbol() -> Result<()> {
        let file = File::open("test_data/spxw.csv")?;
        let mut reader = BufReader::new(file);
        let line = reader.lines().next().unwrap()?;
        assert_eq!(get_symbol(&line), "SPXW220302C04400000");
        Ok(())
    }
}
