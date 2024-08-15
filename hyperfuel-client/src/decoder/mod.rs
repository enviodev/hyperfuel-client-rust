use anyhow::{anyhow, Context, Result};
use fuel_abi_types::abi::program::ProgramABI;
use itertools::Itertools;
use std::{collections::HashMap, fs, path::PathBuf};

pub struct decoder {
    abi: ProgramABI,
    path: String,
}
