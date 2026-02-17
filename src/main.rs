use core::fmt;
use std::{
    ffi::CStr,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use flate2::Compression;
use sha1::{Digest, Sha1};

const GIT_OBJECT_PATH: &'static str = ".git/objects";

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
    /// Hash object file to then write the content on git object
    HashObject {
        /// Write objects to object database
        #[arg(short = 'w')]
        write: String,
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
    const DIRECTORY_LENGTH: usize = 2;
    return (
        object_hash[..DIRECTORY_LENGTH].to_string(),
        object_hash[DIRECTORY_LENGTH..].to_string(),
    );
}

fn get_object_path(dir_path: &str, hash_path: &str) -> Result<String> {
    let full_dir_path = format!("{}/{}", GIT_OBJECT_PATH, dir_path);
    let mut possible_hash_path = Vec::new();

    for entry in fs::read_dir(&full_dir_path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;

        if metadata.is_file() {
            let file_name = entry.file_name().to_string_lossy().to_string();
            if file_name.starts_with(hash_path) {
                possible_hash_path.push(file_name);
            }
        }
    }

    if possible_hash_path.is_empty() {
        bail!(format!("no matching path for hash: {}", hash_path));
    } else if possible_hash_path.len() > 1 {
        bail!(format!(
            "multiple matching for hash: {} please specify",
            hash_path
        ));
    }

    return Ok(format!("{}/{}", full_dir_path, possible_hash_path[0]));
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
    let object_path = get_object_path(&dir_path, &object_path)?;
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

fn write_object_hash(object_hash_type: ObjectHashTypes) -> Result<String> {
    match object_hash_type {
        ObjectHashTypes::Blob(content) => {
            let meta_data = format!("blob {}\0", content.len());
            let mut hasher = Sha1::new();

            hasher.update(&meta_data);
            hasher.update(&content);

            let hash_object = format!("{:x}", hasher.finalize());
            let (dir_path, hash) = get_path_from_hash(&hash_object);
            let full_dir_path = format!("{}/{}", GIT_OBJECT_PATH, dir_path);
            let full_path = format!("{}/{}", full_dir_path, hash);

            fs::create_dir_all(&full_dir_path)
                .context(format!("creating directory {} failed", &full_dir_path))?;

            if std::path::Path::new(&full_path).exists() {
                return Ok(hash_object);
            }

            let file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&full_path)
                .context(format!("opening {} failed", &full_path))?;

            let buf_writer = BufWriter::new(file);
            let mut zlib_encoder =
                flate2::write::ZlibEncoder::new(buf_writer, Compression::default());

            let _ = zlib_encoder.write(meta_data.as_bytes());
            let _ = zlib_encoder.write(content.as_bytes());

            Ok(hash_object)
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.subcommands {
        Commands::CatFile {
            pretty_print,
            exist,
        } => match (pretty_print, exist) {
            (None, Some(hash)) => {
                let (dir_path, object_path) = get_path_from_hash(&hash);
                let object_hash = get_object_path(&dir_path, &object_path);
                match object_hash {
                    Ok(full_object_hash) => {
                        println!("object hash exist, with path: {}", full_object_hash)
                    }
                    Err(err) => println!("{}", err),
                }
            }
            (Some(hash), None) => {
                let object_hash = parse_object_hash(&hash)?;
                print!("{}", object_hash);
            }
            (_, _) => {
                println!("invalid command")
            }
        },
        Commands::HashObject { write } => {
            let content =
                String::from_utf8(fs::read(&write).context(format!("failed reading {}", &write))?)
                    .context("failed parsing to string")?;
            let hash_object = write_object_hash(ObjectHashTypes::Blob(content))?;
            println!("written object hash: {}", hash_object);
        }
    }
    Ok(())
}
