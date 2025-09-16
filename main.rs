use bon::Builder;
use eyre::{Error, Result};
use mimalloc::MiMalloc;
use std::path::Path;
use tap::prelude::*;
use tokio::fs::File;
use tracing::{Level, info};
use vortex::{
  ToCanonical,
  arrays::{PrimitiveArray, StructArray},
  file::{VortexOpenOptions, VortexWriteOptions},
  iter::ArrayIteratorExt,
  validity::Validity,
};

#[global_allocator]
static GLOBAL_ALLOC: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
  tracing_subscriber::fmt().with_max_level(Level::INFO).init();
  let out = std::env::var("OUT")?;
  let out = Path::new(&out);
  let messages: Vec<_> = (0..10)
    .map(|i| Message::builder().a(i).b(-i).build())
    .collect();
  info!(?out, ?messages);
  let messages = Messages(messages)
    .try_conv::<StructArray>()?
    .tap(|messages| info!(?messages, tree = %messages.display_tree(), values = %messages.display_values()));
  let mut file = File::create(out.join("messages.vortex")).await?;
  VortexWriteOptions::default()
    .write(&mut file, messages.to_array_stream())
    .await?;
  let duckdb = VortexOpenOptions::file()
    .open(out.join("duckdb.vortex"))
    .await?
    .scan()?
    .into_array_iter()?
    .read_all()?
    .to_struct()
    .tap(|duckdb| info!(?duckdb, tree = %duckdb.display_tree(), values = %duckdb.display_values()));
  Messages::try_from(duckdb)?.tap(|messages| info!(?messages));
  Ok(())
}

#[derive(Debug)]
struct Messages(Vec<Message>);

impl TryFrom<StructArray> for Messages {
  type Error = Error;
  fn try_from(array: StructArray) -> Result<Self, Self::Error> {
    let a = array.field_by_name("a")?.to_primitive();
    let b = array.field_by_name("b")?.to_primitive();
    a.as_slice()
      .into_iter()
      .copied()
      .zip(b.as_slice().into_iter().copied())
      .map(|(a, b)| Message::builder().a(a).b(b).build())
      .collect::<Vec<_>>()
      .pipe(Self)
      .pipe(Ok)
  }
}

impl TryFrom<Messages> for StructArray {
  type Error = Error;
  fn try_from(Messages(messages): Messages) -> Result<Self, Self::Error> {
    let length = messages.len();
    let mut a = vec![0; length];
    let mut b = vec![0; length];
    messages.into_iter().enumerate().for_each(|(i, message)| {
      a[i] = message.a;
      b[i] = message.b;
    });
    Ok(StructArray::try_new(
      ["a", "b"].into(),
      vec![
        PrimitiveArray::from_iter(a).into(),
        PrimitiveArray::from_iter(b).into(),
      ],
      length,
      Validity::NonNullable,
    )?)
  }
}

#[derive(Debug, Builder)]
struct Message {
  a: i64,
  b: i64,
}
