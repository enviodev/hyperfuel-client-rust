use std::collections::BTreeSet;

use anyhow::{Context, Result};

use arrow2::datatypes::Schema;

pub fn project_schema(
    schema: &Schema,
    field_selection: &BTreeSet<String>,
) -> Result<Schema, anyhow::Error> {
    let mut select_indices = Vec::new();
    for col_name in field_selection.iter() {
        let (idx, _) = schema
            .fields
            .iter()
            .enumerate()
            .find(|(_, f)| &f.name == col_name)
            .context(format!("couldn't find column {col_name} in schema"))?;
        select_indices.push(idx);
    }

    let schema: Schema = schema
        .fields
        .iter()
        .filter(|f| field_selection.contains(&f.name))
        .cloned()
        .collect::<Vec<_>>()
        .into();

    Ok(schema)
}
