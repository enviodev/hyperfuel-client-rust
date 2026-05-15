use std::sync::Arc;

use polars_arrow::{
    array::{
        ArrayFromIter, BinaryArray, BinaryViewArray, MutableUtf8Array, Utf8Array, Utf8ViewArray,
    },
    datatypes::{ArrowDataType as DataType, ArrowSchema as Schema, Field},
};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};

use crate::{ArrowBatch, ArrowChunk};

pub fn hex_encode_prefixed(bytes: &[u8]) -> String {
    let mut out = vec![0; bytes.len() * 2 + 2];

    out[0] = b'0';
    out[1] = b'x';

    faster_hex::hex_encode(bytes, &mut out[2..]).unwrap();

    unsafe { String::from_utf8_unchecked(out) }
}

pub fn hex_encode_batch<F: Fn(&[u8]) -> String + Send + Sync + Copy>(
    batch: &ArrowBatch,
    encode: F,
) -> ArrowBatch {
    let (fields, cols) = batch
        .chunk
        .columns()
        .par_iter()
        .zip(batch.schema.fields.par_iter())
        .map(|(col, field)| {
            let col = match col.data_type() {
                DataType::Binary => {
                    Box::new(hex_encode(col.as_any().downcast_ref().unwrap(), encode))
                }
                _ => col.clone(),
            };

            (
                Field::new(
                    field.name.clone(),
                    col.data_type().clone(),
                    field.is_nullable,
                ),
                col,
            )
        })
        .collect::<(Vec<_>, Vec<_>)>();

    ArrowBatch {
        chunk: ArrowChunk::new(cols).into(),
        schema: Schema::from(fields).into(),
    }
}

fn hex_encode<F: Fn(&[u8]) -> String + Copy>(
    input: &BinaryArray<i32>,
    encode: F,
) -> Utf8Array<i32> {
    let mut arr = MutableUtf8Array::<i32>::new();

    for buf in input.iter() {
        arr.push(buf.map(encode));
    }

    arr.into()
}

pub fn map_batch_to_binary_view(batch: ArrowBatch) -> ArrowBatch {
    let cols = batch
        .chunk
        .arrays()
        .iter()
        .map(|col| match col.data_type() {
            DataType::Binary => BinaryViewArray::arr_from_iter(
                col.as_any()
                    .downcast_ref::<BinaryArray<i32>>()
                    .unwrap()
                    .iter(),
            )
            .boxed(),
            DataType::Utf8 => Utf8ViewArray::arr_from_iter(
                col.as_any()
                    .downcast_ref::<Utf8Array<i32>>()
                    .unwrap()
                    .iter(),
            )
            .boxed(),
            _ => col.clone(),
        })
        .collect::<Vec<_>>();

    let fields = cols
        .iter()
        .zip(batch.schema.fields.iter())
        .map(|(col, field)| {
            Field::new(
                field.name.clone(),
                col.data_type().clone(),
                field.is_nullable,
            )
        })
        .collect::<Vec<_>>();

    let schema = Schema {
        fields,
        metadata: Default::default(),
    };

    ArrowBatch {
        chunk: Arc::new(ArrowChunk::new(cols)),
        schema: Arc::new(schema),
    }
}
