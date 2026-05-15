use std::{collections::VecDeque, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use hyperfuel_net_types::Query;
use hyperfuel_schema::concat_chunks;
use polars_arrow::{datatypes::ArrowSchema as Schema, legacy::error::PolarsError};
use polars_parquet::parquet::write::FileStreamer;
use polars_parquet::write::StatisticsOptions;
use polars_parquet::{
    read::ParquetError,
    write::{
        array_to_columns, to_parquet_schema, to_parquet_type, transverse, CompressedPage, DynIter,
        DynStreamingIterator, Encoding, FallibleStreamingIterator,
        RowGroupIterColumns as RowGroupIter, WriteOptions,
    },
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::{
    config::StreamConfig, rayon_async, util::map_batch_to_binary_view, ArrowBatch, Client,
};

pub async fn collect_parquet(
    client: Arc<Client>,
    path: &str,
    query: Query,
    config: StreamConfig,
) -> Result<()> {
    let path = PathBuf::from(path);

    tokio::fs::create_dir_all(&path)
        .await
        .context("create parquet dir")?;

    let mut blocks_path = path.clone();
    blocks_path.push("blocks.parquet");
    let (mut blocks_sender, blocks_join) = spawn_writer(blocks_path)?;

    let mut transactions_path = path.clone();
    transactions_path.push("transactions.parquet");
    let (mut transactions_sender, transactions_join) = spawn_writer(transactions_path)?;

    let mut receipts_path = path.clone();
    receipts_path.push("receipts.parquet");
    let (mut receipts_sender, receipts_join) = spawn_writer(receipts_path)?;

    let mut inputs_path = path.clone();
    inputs_path.push("inputs.parquet");
    let (mut inputs_sender, inputs_join) = spawn_writer(inputs_path)?;

    let mut outputs_path = path.clone();
    outputs_path.push("outputs.parquet");
    let (mut outputs_sender, outputs_join) = spawn_writer(outputs_path)?;

    let mut rx = client
        .stream_arrow(query, config)
        .await
        .context("start stream")?;

    while let Some(resp) = rx.recv().await {
        let resp = resp.context("get query response")?;

        log::trace!("got data up to block {}", resp.next_block);

        let blocks_fut = async move {
            for batch in resp.data.blocks {
                blocks_sender
                    .send(batch)
                    .await
                    .context("write blocks chunk to parquet")?;
            }

            Ok::<_, anyhow::Error>(blocks_sender)
        };

        let txs_fut = async move {
            for batch in resp.data.transactions {
                transactions_sender
                    .send(batch)
                    .await
                    .context("write transactions chunk to parquet")?;
            }

            Ok::<_, anyhow::Error>(transactions_sender)
        };

        let receipts_fut = async move {
            for batch in resp.data.receipts {
                receipts_sender
                    .send(batch)
                    .await
                    .context("write receipts chunk to parquet")?;
            }

            Ok::<_, anyhow::Error>(receipts_sender)
        };

        let inputs_fut = async move {
            for batch in resp.data.inputs {
                inputs_sender
                    .send(batch)
                    .await
                    .context("write inputs chunk to parquet")?;
            }

            Ok::<_, anyhow::Error>(inputs_sender)
        };

        let outputs_fut = async move {
            for batch in resp.data.outputs {
                outputs_sender
                    .send(batch)
                    .await
                    .context("write outputs chunk to parquet")?;
            }

            Ok::<_, anyhow::Error>(outputs_sender)
        };

        // Execute futures concurrently
        (
            blocks_sender,
            transactions_sender,
            receipts_sender,
            inputs_sender,
            outputs_sender,
        ) = tokio::try_join!(blocks_fut, txs_fut, receipts_fut, inputs_fut, outputs_fut)?;
    }

    std::mem::drop(blocks_sender);
    std::mem::drop(transactions_sender);
    std::mem::drop(receipts_sender);
    std::mem::drop(inputs_sender);
    std::mem::drop(outputs_sender);

    blocks_join
        .await
        .context("join blocks task")?
        .context("finish blocks file")?;
    transactions_join
        .await
        .context("join transactions task")?
        .context("finish transactions file")?;
    receipts_join
        .await
        .context("join receipts task")?
        .context("finish receipts file")?;
    inputs_join
        .await
        .context("join inputs task")?
        .context("finish inputs file")?;
    outputs_join
        .await
        .context("join outputs task")?
        .context("finish outputs file")?;

    Ok(())
}

fn spawn_writer(path: PathBuf) -> Result<(mpsc::Sender<ArrowBatch>, JoinHandle<Result<()>>)> {
    let (tx, rx) = mpsc::channel(64);

    let handle = tokio::task::spawn(async move {
        match run_writer(rx, path).await {
            Ok(v) => Ok(v),
            Err(e) => {
                log::error!("failed to run parquet writer: {:?}", e);
                Err(e)
            }
        }
    });

    Ok((tx, handle))
}

async fn run_writer(mut rx: mpsc::Receiver<ArrowBatch>, path: PathBuf) -> Result<()> {
    let make_writer = move |schema: &Schema| {
        let schema = schema.clone();
        let path = path.clone();
        async move {
            let write_options = polars_parquet::parquet::write::WriteOptions {
                write_statistics: true,
                version: polars_parquet::parquet::write::Version::V2,
            };

            let file = tokio::io::BufWriter::new(
                tokio::fs::File::create(&path)
                    .await
                    .context("create parquet file")?,
            )
            .compat();

            let parquet_schema = to_parquet_schema(&schema).context("to parquet schema")?;

            let writer = FileStreamer::new(file, parquet_schema, write_options, None);

            Ok::<_, anyhow::Error>(writer)
        }
    };

    let mut writer = None;

    let num_cpus = num_cpus::get();
    let mut encode_jobs = VecDeque::<EncodeFut>::with_capacity(num_cpus);

    let mut data = Vec::new();
    let mut total_rows = 0;
    loop {
        let mut stop = false;
        if let Some(batch) = rx.recv().await {
            total_rows += batch.chunk.len();
            data.push(batch);
        } else {
            stop = true;
        }

        if !data.is_empty() && (stop || total_rows >= ROW_GROUP_MAX_ROWS) {
            let batches = std::mem::take(&mut data);
            if encode_jobs.len() >= num_cpus {
                let fut = encode_jobs.pop_front().unwrap();
                let (rg, schema) = fut
                    .await
                    .context("join prepare task")?
                    .context("prepare row group")?;
                if writer.is_none() {
                    writer = Some(make_writer(&schema).await.context("create writer")?);
                }
                writer
                    .as_mut()
                    .unwrap()
                    .write(rg)
                    .await
                    .context("write encoded row group to file")?;
            }

            total_rows = 0;
            let schema = batches[0].schema.clone();
            let chunks = batches.into_iter().map(|b| b.chunk).collect::<Vec<_>>();
            let chunk = concat_chunks(chunks.as_slice()).context("concat chunks")?;
            let batch = ArrowBatch {
                chunk: Arc::new(chunk),
                schema: schema.clone(),
            };
            let batch = map_batch_to_binary_view(batch);

            let fut = rayon_async::spawn(move || {
                let rg = encode_row_group(
                    batch,
                    WriteOptions {
                        statistics: StatisticsOptions::default(),
                        version: polars_parquet::write::Version::V2,
                        compression: polars_parquet::write::CompressionOptions::Lz4Raw,
                        data_page_size: None,
                    },
                )
                .context("encode row group")?;

                Ok((rg, schema))
            });

            encode_jobs.push_back(fut);
        }

        if stop {
            break;
        }
    }

    while let Some(fut) = encode_jobs.pop_front() {
        let (rg, schema) = fut
            .await
            .context("join prepare task")?
            .context("prepare row group")?;
        if writer.is_none() {
            writer = Some(make_writer(&schema).await.context("create writer")?);
        }
        writer
            .as_mut()
            .unwrap()
            .write(rg)
            .await
            .context("write encoded row group to file")?;
    }

    if let Some(writer) = writer.as_mut() {
        let _size = writer.end(None).await.context("write footer")?;
    }

    Ok(())
}

type EncodeFut = tokio::sync::oneshot::Receiver<
    Result<(
        DynIter<
            'static,
            std::result::Result<
                DynStreamingIterator<'static, CompressedPage, PolarsError>,
                PolarsError,
            >,
        >,
        Arc<Schema>,
    )>,
>;

fn encode_row_group(
    batch: ArrowBatch,
    write_options: WriteOptions,
) -> Result<RowGroupIter<'static, PolarsError>> {
    let fields = batch
        .schema
        .fields
        .iter()
        .map(|field| to_parquet_type(field).context("map to parquet field"))
        .collect::<Result<Vec<_>>>()?;
    let encodings = batch
        .schema
        .fields
        .iter()
        .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
        .collect::<Vec<_>>();

    let data = batch
        .chunk
        .arrays()
        .iter()
        .zip(fields)
        .zip(encodings)
        .flat_map(move |((array, type_), encoding)| {
            let encoded_columns = array_to_columns(array, type_, write_options, &encoding).unwrap();
            encoded_columns
                .into_iter()
                .map(|encoded_pages| {
                    let pages = encoded_pages;

                    let pages = DynIter::new(
                        pages
                            .into_iter()
                            .map(|x| x.map_err(|e| ParquetError::OutOfSpec(e.to_string()))),
                    );

                    let compressed_pages = pages
                        .map(|page| {
                            let page = page?;
                            polars_parquet::write::compress(page, vec![], write_options.compression)
                                .map_err(PolarsError::from)
                        })
                        .collect::<Vec<_>>();

                    Ok(DynStreamingIterator::new(CompressedPageIter {
                        data: compressed_pages.into_iter(),
                        current: None,
                    }))
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    Ok(DynIter::new(data.into_iter()))
}

struct CompressedPageIter {
    data: std::vec::IntoIter<std::result::Result<CompressedPage, PolarsError>>,
    current: Option<CompressedPage>,
}

impl FallibleStreamingIterator for CompressedPageIter {
    type Item = CompressedPage;
    type Error = PolarsError;

    fn get(&self) -> Option<&Self::Item> {
        self.current.as_ref()
    }

    fn advance(&mut self) -> std::result::Result<(), Self::Error> {
        self.current = match self.data.next() {
            Some(page) => Some(page?),
            None => None,
        };
        Ok(())
    }
}

const ROW_GROUP_MAX_ROWS: usize = 10_000;
