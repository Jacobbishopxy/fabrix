//! Core FMT
//!
//! Consider using https://github.com/nukesor/comfy-table to format dataframe.

use std::fmt::{Debug, Display};

use comfy_table::{Attribute, Cell, Table};

use super::{DataFrame, Row, Series};

impl From<Row> for Vec<Cell> {
    fn from(row: Row) -> Self {
        let mut cells = vec![Cell::new(row.index().to_string()).add_attribute(Attribute::Italic)];

        row.data().iter().for_each(|value| {
            cells.push(Cell::new(value.to_string()));
        });

        cells
    }
}

impl Display for Series {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl Debug for Series {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self, f)
    }
}

impl Display for DataFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut table = Table::new();

        let mut headers =
            vec![Cell::new(self.index.name().to_string()).add_attribute(Attribute::Italic)];
        let mut table_head = self
            .get_column_names()
            .iter()
            .map(|name| Cell::new(name.to_string()))
            .collect::<Vec<_>>();
        headers.append(&mut table_head);

        table.set_header(headers);

        self.clone().into_iter().for_each(|row| {
            let cells: Vec<Cell> = row.into();
            table.add_row(cells);
        });

        let shape = self.shape();
        write!(f, "Shape: {:?}\n{table}", shape)
    }
}

impl Debug for DataFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self, f)
    }
}

#[cfg(test)]
mod test_fmt {
    use crate::df;

    #[test]
    fn test_df_display() {
        let df = df![
            "names" => ["Jacob", "Sam", "Jason"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();

        println!("{df}");
    }
}
