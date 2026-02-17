use core::fmt;
use std::{
    ffi::CStr,
    fs::File,
    io::{BufRead, BufReader, Read},
};

use anyhow::{Context, Ok, Result, bail};
use clap::{Parser, Subcommand};

#[derive(Debug, Subcommand)]
enum Commands {
    /// check the content of hash object
    CatFile {
        /// pretty print the object content
        #[arg(short = 'p', conflicts_with = "exist")]
        pretty_print: Option<String>,
        /// check if the object hash exist
        #[arg(short = 'e', conflicts_with = "pretty_print")]
        exist: Option<String>,
    },
}

#[derive(Debug, Parser)]
#[command(name = "git")]
#[command(about = "a partial implementation of git for learning purposes only")]
struct Cli {
    #[command(subcommand)]
    subcommands: Commands,
}

fn get_path_from_hash(object_hash: &str) -> (String, String) {
    return (object_hash[..2].to_string(), object_hash[2..].to_string());
}

fn get_object_path(dir_path: &str, hash_path: &str) -> String {
    const GIT_OBJECT_PATH: &'static str = ".git/objects";
    return format!("{}/{}/{}", GIT_OBJECT_PATH, dir_path, hash_path);
}

enum ObjectHashTypes {
    Blob(String),
    // fill in as needed
}

impl fmt::Display for ObjectHashTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectHashTypes::Blob(content) => write!(f, "{}", content),
        }
    }
}

fn parse_object_hash(hash_object: &str) -> Result<ObjectHashTypes> {
    let (dir_path, object_path) = get_path_from_hash(&hash_object);
    let object_path = get_object_path(&dir_path, &object_path);
    let file =
        File::open(&object_path).context(format!("failed to read object path {}", &object_path))?;

    let (mut buf_reader, mut buffer) = (
        BufReader::new(flate2::read::ZlibDecoder::new(file)),
        Vec::new(),
    );
    // git hash object file will always have the following format:
    // <type> <blob_size>/0<content>
    // therefore we will read until find /0 and parse the type and put the content
    let _ = buf_reader.read_until(b'\0', &mut buffer);
    let meta_data = CStr::from_bytes_with_nul(&buffer)
        .context(format!(
            "failed parsing metadata from object_hash: {}",
            &object_path
        ))?
        .to_str()?;

    let mut meta_data_split = meta_data.split(" ");
    let (content_type, content_size) = (
        meta_data_split
            .next()
            .with_context(|| format!("no content type found on metadata: {}", &meta_data))?,
        meta_data_split
            .next()
            .with_context(|| format!("no size found on metadata {}", &meta_data))?,
    );

    let content_size: usize = content_size
        .parse()
        .with_context(|| format!("failed to parse content size, found {}", content_size))?;

    let mut buffer = vec![0; content_size];
    let _ = buf_reader.read_exact(&mut buffer);

    match content_type {
        "blob" => Ok(ObjectHashTypes::Blob(
            String::from_utf8(buffer).context("parsing buffer to string utf-8")?,
        )),
        _ => bail!(format!("unsupported type: {}", content_type)),
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.subcommands {
        Commands::CatFile {
            pretty_print,
            exist,
        } => match (pretty_print, exist) {
            (None, None) => {
                println!("none were filled");
            }
            (None, Some(hash)) => {
                let object_hash = parse_object_hash(&hash)?;
                println!("{}", object_hash);
            }
            (Some(hash), None) => {
                let object_hash = parse_object_hash(&hash)?;
                print!("{}", object_hash);
            }
            (Some(_), Some(_)) => {
                println!("both somehow exists!")
            }
        },
    }
    Ok(())
}
