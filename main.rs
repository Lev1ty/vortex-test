use bon::Builder;
use eyre::{Error, OptionExt, Result};
use mimalloc::MiMalloc;
use std::path::Path;
use tap::prelude::*;
use tokio::fs::File;
use tracing::{Level, info};
use vortex::{
  ToCanonical,
  arrays::{PrimitiveArray, StructArray},
  buffer::Buffer,
  builders::StructBuilder,
  dtype::Nullability,
  file::{VortexOpenOptions, VortexWriteOptions},
  iter::ArrayIteratorExt,
  scalar::StructScalar,
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
  let mut builder = StructBuilder::new(duckdb.struct_fields().clone(), Nullability::NonNullable);
  duckdb.append_to_builder(&mut builder);
  Messages::try_from(&duckdb)?.tap(|messages| info!(?messages));
  messages.append_to_builder(&mut builder);
  let mut file = File::create(out.join("combined.vortex")).await?;
  builder
    .finish_into_struct()
    .tap(|combined| info!(?combined, tree = %combined.display_tree(), values = %combined.display_values()))
    .to_array_stream()
    .pipe(|stream| VortexWriteOptions::default().write(&mut file, stream))
    .await?;
  let scalar = messages.scalar_at(0);
  let message = scalar.as_struct().try_conv::<Message>()?;
  info!(?scalar, ?message);
  Ok(())
}

#[derive(Debug)]
struct Messages(Vec<Message>);

impl TryFrom<&StructArray> for Messages {
  type Error = Error;
  fn try_from(array: &StructArray) -> Result<Self, Self::Error> {
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
        PrimitiveArray::new(Buffer::from_iter(a), Validity::AllValid).into(),
        PrimitiveArray::new(Buffer::from_iter(b), Validity::AllValid).into(),
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

impl TryFrom<StructScalar<'_>> for Message {
  type Error = Error;
  fn try_from(scalar: StructScalar) -> Result<Self, Self::Error> {
    let a = scalar
      .field("a")
      .ok_or_eyre("a:field")?
      .as_primitive()
      .typed_value::<i64>()
      .ok_or_eyre("a:typed_value")?;
    let b = scalar
      .field("b")
      .ok_or_eyre("b:field")?
      .as_primitive()
      .typed_value::<i64>()
      .ok_or_eyre("b:typed_value")?;
    Ok(Message::builder().a(a).b(b).build())
  }
}
