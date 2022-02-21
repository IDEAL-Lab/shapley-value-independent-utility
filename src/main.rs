#[macro_use]
extern crate tracing;

use anyhow::{bail, Result};
use serde_json::json;
use shapley_value::*;
use std::{fs::File, io::BufWriter, path::PathBuf};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Opts {
    /// Dataset name
    #[structopt(short = "-d", long)]
    name: String,

    /// Input csv directory
    #[structopt(short = "i", long, parse(from_os_str))]
    csv_dir: PathBuf,

    /// Input meta directory
    #[structopt(short, long, parse(from_os_str))]
    meta_dir: PathBuf,

    /// Output file
    #[structopt(short, long, parse(from_os_str))]
    output: PathBuf,

    /// Scheme name
    #[structopt(short, long)]
    scheme: String,

    /// Number of threads
    #[structopt(short, long)]
    num_threads: Option<usize>,
}

fn main() -> Result<()> {
    utils::init_tracing_subscriber("info")?;
    let opts = Opts::from_args();
    info!("opts: {:#?}", opts);
    utils::setup_rayon(opts.num_threads)?;

    let dataset = DataSet::load(&opts.name, &opts.csv_dir, &opts.meta_dir)?;

    let result = match opts.scheme.as_str() {
        "traditional" => alg::traditional::traditional_scheme(&dataset)?,
        _ => bail!("unknown scheme"),
    };

    let mut result_json = serde_json::to_value(result)?;
    result_json.as_object_mut().unwrap().append(
        &mut json!({
            "dataset": opts.name,
            "scheme": opts.scheme,
            "csv_dir": opts.csv_dir,
            "meta_dir": opts.meta_dir,
            "num_threads": opts.num_threads,
        })
        .as_object_mut()
        .unwrap(),
    );

    let out = BufWriter::new(File::create(&opts.output)?);
    serde_json::to_writer(out, &result_json)?;

    Ok(())
}
