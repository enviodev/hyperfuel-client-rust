use std::collections::BTreeSet;

use anyhow::{anyhow, Result};
use polars_arrow::datatypes::ArrowSchema as Schema;

/// Project a schema down to the named fields. Missing field names are silently
/// dropped from the result.
///
/// Prefer [`try_project_schema`], which returns an error listing any unknown
/// names. This non-erroring variant is retained for backward compatibility.
pub fn project_schema(schema: &Schema, field_selection: &BTreeSet<String>) -> Schema {
    schema
        .fields
        .iter()
        .filter(|f| field_selection.contains(&f.name))
        .cloned()
        .collect::<Vec<_>>()
        .into()
}

/// Project a schema down to the named fields, returning an error if any
/// requested field is not present in the source schema. Use this to catch
/// typos in field selections instead of silently receiving a partial schema.
pub fn try_project_schema(schema: &Schema, field_selection: &BTreeSet<String>) -> Result<Schema> {
    let known: BTreeSet<&String> = schema.fields.iter().map(|f| &f.name).collect();
    let missing: Vec<&String> = field_selection
        .iter()
        .filter(|name| !known.contains(name))
        .collect();
    if !missing.is_empty() {
        return Err(anyhow!(
            "selected columns not found in schema: {}",
            missing
                .into_iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    Ok(project_schema(schema, field_selection))
}
