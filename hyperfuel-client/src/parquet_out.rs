use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use arrow2::{
    datatypes::Schema,
    io::parquet::write::{transverse, Encoding, FileSink, WriteOptions},
};
use futures::SinkExt;
use hyperfuel_net_types::Query;
use hyperfuel_schema::project_schema;
use tokio::fs::File;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::Client;

pub async fn create_parquet_folder(client: &Client, query: Query, path: String) -> Result<()> {
    let mut query = query;

    let height = client
        .get_height_with_retry()
        .await
        .context("get height of source")?;

    let to_block = match query.to_block {
        Some(to_block) => std::cmp::min(to_block, height),
        None => height,
    };

    let mut path = PathBuf::from(path);

    tokio::fs::create_dir_all(&path)
        .await
        .context("create parquet dir")?;

    path.push("block.parquet");
    let mut blocks = make_file_sink(
        &path,
        &hyperfuel_schema::block_header(),
        &query.field_selection.block,
    )
    .await
    .context("create blocks output parquet")?;
    path.pop();

    path.push("transaction.parquet");
    let mut txs = make_file_sink(
        &path,
        &hyperfuel_schema::transaction(),
        &query.field_selection.transaction,
    )
    .await
    .context("create transactions output parquet")?;
    path.pop();

    path.push("receipt.parquet");
    let mut receipts = make_file_sink(
        &path,
        &hyperfuel_schema::receipt(),
        &query.field_selection.receipt,
    )
    .await
    .context("create receipts output parquet")?;
    path.pop();

    path.push("input.parquet");
    let mut inputs = make_file_sink(
        &path,
        &hyperfuel_schema::input(),
        &query.field_selection.input,
    )
    .await
    .context("create inputs output parquet")?;
    path.pop();

    path.push("output.parquet");
    let mut outputs = make_file_sink(
        &path,
        &hyperfuel_schema::output(),
        &query.field_selection.output,
    )
    .await
    .context("create outputs output parquet")?;
    path.pop();

    loop {
        let resp = client
            .get_arrow_data_with_retry(&query)
            .await
            .context("send query")?;

        for batch in resp.data.blocks {
            blocks
                .send(batch.chunk)
                .await
                .context("write blocks chunk to parquet")?;
        }

        for batch in resp.data.transactions {
            txs.send(batch.chunk)
                .await
                .context("write transactions chunk to parquet")?;
        }

        for batch in resp.data.receipts {
            receipts
                .send(batch.chunk)
                .await
                .context("write receipts chunk to parquet")?;
        }

        for batch in resp.data.inputs {
            inputs
                .send(batch.chunk)
                .await
                .context("write inputs chunk to parquet")?;
        }

        for batch in resp.data.outputs {
            outputs
                .send(batch.chunk)
                .await
                .context("write outputs chunk to parquet")?;
        }

        if resp.next_block >= to_block {
            break;
        } else {
            query.from_block = resp.next_block;
        }
    }

    blocks
        .close()
        .await
        .context("finish writing blocks parquet")?;
    txs.close()
        .await
        .context("finish writing transactions parquet")?;
    receipts
        .close()
        .await
        .context("finish writing receipts parquet")?;
    inputs
        .close()
        .await
        .context("finish writing inputs parquet")?;
    outputs
        .close()
        .await
        .context("finish writing outputs parquet")?;

    Ok(())
}

async fn make_file_sink(
    path: &Path,
    schema: &Schema,
    field_selection: &BTreeSet<String>,
) -> Result<FileSink<'static, Compat<File>>> {
    let file = tokio::fs::File::create(path)
        .await
        .context("create parquet file")?
        .compat_write();

    let schema = project_schema(schema, field_selection).context("project schema")?;

    let encodings = schema
        .fields
        .iter()
        .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
        .collect();

    let file_sink = FileSink::try_new(
        file,
        schema,
        encodings,
        WriteOptions {
            write_statistics: true,
            version: arrow2::io::parquet::write::Version::V2,
            compression: arrow2::io::parquet::write::CompressionOptions::Lz4Raw,
            data_pagesize_limit: None,
        },
    )
    .context("create file sink")?;

    Ok(file_sink)
}
