use std::collections::BTreeSet;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::rc::Rc;
use std::str::FromStr;

use color_eyre::eyre::eyre;
use color_eyre::Result;
use log::warn;
use polars::datatypes::CategoricalChunkedBuilder;
use polars::frame::DataFrame;
use polars::prelude::NamedFromOwned;
use polars::series::{IntoSeries, Series};
use polars_io::parquet::{BatchedWriter, ParquetWriter};

type Row<'a> = [&'a str; 15];

struct Categorical<'a> {
    cached: BTreeSet<Rc<str>>,
    inner: CategoricalChunkedBuilder<'a>,
    len: usize,
}

impl Categorical<'_> {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            cached: BTreeSet::new(),
            inner: CategoricalChunkedBuilder::new("", capacity),
            len: 0,
        }
    }

    fn push(&mut self, entry: &str) -> Result<()> {
        if !self.cached.contains(entry) {
            self.cached.insert(Rc::from(entry));
        }
        let rc = self.cached.get(entry).unwrap();
        let p = Rc::as_ptr(rc);
        self.inner.append_value(unsafe { &*p });
        self.len += 1;
        Ok(())
    }

    fn into_series(self, name: &str) -> Series {
        let mut s = self.inner.finish().into_series();
        s.rename(name);
        s
    }
}

struct Numerical<T> {
    inner: Vec<T>,
}

impl<T> Numerical<T> {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Vec::with_capacity(capacity),
        }
    }

    fn into_series(self, name: &str) -> Series
    where
        Series: NamedFromOwned<Vec<T>>,
    {
        Series::from_vec(name, self.inner)
    }
}

impl<T> Numerical<T>
where
    T: FromStr,
    <T as FromStr>::Err: Error + Send + Sync + 'static,
{
    fn push(&mut self, entry: &str) -> Result<()> {
        self.inner.push(entry.parse()?);
        Ok(())
    }
}

macro_rules! define_columns {
    ($($name:ident: $type:ty, $index:expr),*) => {
        struct Columns {
            $($name: $type),*,
            len: usize,
        }

        impl Columns {
            fn with_capacity(capacity: usize) -> Self {
                Self {
                    $($name: <$type>::with_capacity(capacity)),*,
                    len: 0,
                }
            }

            fn into_cols(self) -> Vec<Series> {
                vec![
                    $(self.$name.into_series(stringify!($name))),*
                ]
            }

            fn push(&mut self, row: Row) -> Result<()> {
                $(
                    self.$name.push(row[$index])?;
                )*
                self.len += 1;
                Ok(())
            }
        }
    };
}

define_columns! {
    c1: Categorical<'static>, 0,
    c2: Numerical<u64>, 1,
    c3: Numerical<u32>, 2,
    c4: Numerical<u32>, 3,
    c5: Categorical<'static>, 4,
    c6: Categorical<'static>, 5,
    c7: Numerical<u32>, 6,
    c8: Numerical<f32>, 7,
    c9: Categorical<'static>, 8,
    c10: Categorical<'static>, 9,
    c11: Numerical<u32>, 10,
    c12: Numerical<f32>, 11,
    c13: Categorical<'static>, 12,
    c14: Categorical<'static>, 13,
    c15: Categorical<'static>, 14
}

pub(crate) struct Product {
    path: PathBuf,
    capacity: usize,
    columns: Columns,
    writer: Option<BatchedWriter<File>>,
}

impl Product {
    pub(crate) fn new(path: PathBuf, capacity: usize) -> Self {
        let columns = Columns::with_capacity(capacity);
        Self {
            path,
            capacity,
            columns,
            writer: None,
        }
    }

    pub(crate) fn push(&mut self, row: &str) -> Result<()> {
        if self.columns.len >= self.capacity {
            self.write()?;
        }
        let t = row.get(0..2).unwrap();
        let row = match t {
            "F@" => Self::parse_quote(row),
            "FT" => Self::parse_trade(row),
            _ => unreachable!(),
        };
        match row {
            Ok(row) => self.columns.push(row)?,
            Err(e) => println!("Failed to parse row: {e:?}"),
        };
        Ok(())
    }

    fn parse_quote(row: &str) -> Result<Row> {
        row.split(" ")
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|e| eyre!("Failed to parse quote: {e:?}"))
    }

    fn parse_trade(row: &str) -> Result<Row> {
        let mut row = row.split(" ").collect::<Vec<_>>();
        row.push(" ");
        row.push(" ");
        row.swap(6, 7);
        row.swap(7, 8);
        row[10] = "0";
        row[11] = "0";
        row.try_into()
            .map_err(|e| eyre!("Failed to parse trade: {e:?}"))
    }

    fn write(&mut self) -> Result<()> {
        let path = self.path.clone();
        let mut cols = Columns::with_capacity(self.capacity);
        std::mem::swap(&mut self.columns, &mut cols);
        let series = cols.into_cols();
        let mut df = DataFrame::new(series)?;
        let writer = if let Some(writer) = &mut self.writer {
            writer
        } else {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(path)?;
            let writer = ParquetWriter::new(file).batched(&df.schema())?;
            self.writer = Some(writer);
            self.writer.as_mut().unwrap()
        };
        writer
            .write_batch(&mut df)
            .map_err(|e| eyre!("Failed to write batch: {e:?}"))
    }
}

impl Drop for Product {
    fn drop(&mut self) {
        let _ = self.write();
        if let Some(writer) = &mut self.writer {
            let _ = writer.finish();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, BufRead, BufReader};
    use std::path::Path;

    use tempfile::tempdir;

    use super::*;

    fn test_iter(file_name: &str) -> impl Iterator<Item = io::Result<String>> {
        let path = PathBuf::new().join("test_data").join(file_name);
        let file = OpenOptions::new().read(true).open(path).unwrap();
        BufReader::new(file).lines()
    }

    #[test]
    fn test_write() -> Result<()> {
        color_eyre::install()?;
        pretty_env_logger::init();

        //let out_dir = tempdir()?;
        let out_dir = Path::new("./test_data");
        let reader = test_iter("spxw.csv");
        let mut product = Product::new(out_dir.join("test.parquet"), 10);
        for row in reader {
            product.push(&row?)?;
        }
        Ok(())
    }
}
