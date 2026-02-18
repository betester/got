use core::fmt;
use std::{
    env,
    ffi::CStr,
    fmt::write,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    os::unix::fs::MetadataExt,
};

use anyhow::{Context, Result, bail};
use chrono::{FixedOffset, Utc};
use clap::{Parser, Subcommand};
use flate2::Compression;
use sha1::{Digest, Sha1};

const GIT_OBJECT_PATH: &'static str = ".git/objects";
const NODE_HASH_BYTES_LENGTH: usize = 20;

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
    /// shows the tree on the current working directory based on the hash
    LsTree {
        hash: String,
    },
    WriteTree,
    CommitTree {
        tree_sha: String,
        #[arg(short = 'p')]
        parent_sha: Option<String>,
        #[arg(short = 'm')]
        message: String,
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

// TODO: get the actual author from git config file
fn get_commit_author_name() -> String {
    "test_author".to_string()
}

fn get_commit_author_email() -> String {
    "test_author@gmail.com".to_string()
}

fn get_commit_comitter_email() -> String {
    "test_comitter@gmail.com".to_string()
}

fn get_commit_comitter_name() -> String {
    "test_comitter".to_string()
}

struct TreeNode {
    name: String,
    hash: String,
    mode: u32,
}

impl TreeNode {
    fn mode_str(&self) -> &'static str {
        match self.mode {
            40000 => "tree",
            100644 | 100755 | 120000 => "blob",
            160000 => "commit",
            _ => "blob",
        }
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        let _ =
            write!(bytes, "{} {}\0", self.mode, self.name).context("writing to Vec is infallible");
        let hash_bytes: Vec<u8> = (0..self.hash.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&self.hash[i..i + 2], 16).expect("invalid hex in hash"))
            .collect();
        bytes.extend_from_slice(&hash_bytes);
        Ok(bytes)
    }
}

impl fmt::Display for TreeNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:06} {} {}\t{}",
            self.mode,
            self.mode_str(),
            self.hash,
            self.name
        )
    }
}

struct CommitContent {
    tree_sha: String,
    parent_sha: Option<String>,
    author_name: String,
    author_email: String,
    committer: String,
    committer_email: String,
    message: String,
    timestamp: chrono::DateTime<FixedOffset>,
}

impl fmt::Display for CommitContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = writeln!(f, "tree {}", self.tree_sha);
        if let Some(sha) = &self.parent_sha {
            let _ = writeln!(f, "parent {}", sha);
        }
        let _ = writeln!(
            f,
            "author {} {} {}",
            self.author_name,
            self.author_email,
            self.timestamp.format("%s %z").to_string()
        );
        let _ = writeln!(
            f,
            "committer {} {} {}",
            self.committer,
            self.committer_email,
            self.timestamp.format("%s %z").to_string()
        );

        write!(f, "\n{}", self.message)
    }
}

enum ObjectHashTypes {
    Blob(String),
    Tree(Vec<TreeNode>), // fill in as needed
    Commit(CommitContent),
}

impl fmt::Display for ObjectHashTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectHashTypes::Blob(content) => write!(f, "{}", content),
            ObjectHashTypes::Tree(items) => {
                for tree_node in items {
                    writeln!(f, "{}", tree_node)?;
                }
                Ok(())
            }
            ObjectHashTypes::Commit(commit_content) => {
                return write!(f, "{}", commit_content);
            }
        }
    }
}

fn parse_file_metadata(meta_data: &str) -> Result<(&str, &str)> {
    let mut meta_data_split = meta_data.split(" ");
    let (first_data, second_data) = (
        meta_data_split
            .next()
            .with_context(|| format!("no content type found on metadata: {}", &meta_data))?,
        meta_data_split
            .next()
            .with_context(|| format!("no size found on metadata {}", &meta_data))?,
    );

    return Ok((first_data, second_data));
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

    let (content_type, content_size) = parse_file_metadata(meta_data)?;
    let content_size: usize = content_size
        .parse()
        .with_context(|| format!("failed to parse content size, found {}", content_size))?;

    let mut buffer = vec![0; content_size];
    let _ = buf_reader.read_exact(&mut buffer);

    match content_type {
        "blob" => Ok(ObjectHashTypes::Blob(
            String::from_utf8(buffer).context("parsing buffer to string utf-8")?,
        )),
        "tree" => {
            let (mut position, mut tree_nodes) = (0, Vec::new());
            while position < buffer.len() {
                let null_offset = buffer[position..]
                    .iter()
                    .position(|&b| b == b'\0')
                    .with_context(|| "failed finding nul delimiter for tree nodes content")?;

                let metadata_end = position + null_offset;
                let node_metadata = CStr::from_bytes_with_nul(&buffer[position..=metadata_end])
                    .with_context(|| {
                        format!("failed parsing metadata from object_hash: {}", &object_path)
                    })?
                    .to_str()?;

                let (mode, name) = parse_file_metadata(node_metadata)?;
                let mode: u32 = mode
                    .parse()
                    .with_context(|| format!("failed to parse mode, found {}", mode))?;

                let sha_start = metadata_end + 1;
                let node_hash = buffer[sha_start..sha_start + NODE_HASH_BYTES_LENGTH]
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<String>();

                tree_nodes.push(TreeNode {
                    name: name.to_string(),
                    mode,
                    hash: node_hash,
                });

                position = sha_start + NODE_HASH_BYTES_LENGTH;
            }
            Ok(ObjectHashTypes::Tree(tree_nodes))
        }
        "commit" => {
            let content = String::from_utf8(buffer).context("parsing commit buffer to utf-8")?;

            let lines: Vec<Option<String>> = content
                .split('\n')
                .map(|l| if l.is_empty() { None } else { Some(l.to_string()) })
                .collect();

            let separator_pos = lines
                .iter()
                .position(|l| l.is_none())
                .with_context(|| "failed to find message separator in commit content")?;

            let message = lines[separator_pos + 1..]
                .iter()
                .filter_map(|l| l.as_deref())
                .collect::<Vec<_>>()
                .join("\n");

            let header_lines = &lines[..separator_pos];

            const COMMIT_FIELDS: [&str; 4] = ["tree", "parent", "author", "committer"];

            let find_field = |prefix: &str| -> Option<String> {
                header_lines
                    .iter()
                    .filter_map(|l| l.as_deref())
                    .find(|line| {
                        line.starts_with(prefix)
                            && line.as_bytes().get(prefix.len()) == Some(&b' ')
                    })
                    .map(|line| line[prefix.len() + 1..].to_string())
            };

            let tree_sha = find_field(COMMIT_FIELDS[0])
                .with_context(|| "failed to find tree in commit")?;
            let parent_sha = find_field(COMMIT_FIELDS[1]);
            let author_line = find_field(COMMIT_FIELDS[2])
                .with_context(|| "failed to find author in commit")?;
            let committer_line = find_field(COMMIT_FIELDS[3])
                .with_context(|| "failed to find committer in commit")?;

            let author_parts: Vec<&str> = author_line.split(' ').collect();
            let n = author_parts.len();
            let author_name = author_parts[..n.saturating_sub(3)].join(" ");
            let author_email = author_parts
                .get(n.saturating_sub(3))
                .with_context(|| format!("failed to parse email from author: {}", author_line))?
                .to_string();
            let unix_ts = author_parts
                .get(n.saturating_sub(2))
                .with_context(|| "missing unix timestamp in author")?;
            let tz = author_parts
                .get(n.saturating_sub(1))
                .with_context(|| "missing timezone in author")?;
            let timestamp =
                chrono::DateTime::parse_from_str(&format!("{} {}", unix_ts, tz), "%s %z")
                    .with_context(|| format!("failed to parse timestamp: {} {}", unix_ts, tz))?;

            let committer_parts: Vec<&str> = committer_line.split(' ').collect();
            let m = committer_parts.len();
            let committer = committer_parts[..m.saturating_sub(3)].join(" ");
            let committer_email = committer_parts
                .get(m.saturating_sub(3))
                .with_context(|| {
                    format!("failed to parse email from committer: {}", committer_line)
                })?
                .to_string();

            Ok(ObjectHashTypes::Commit(CommitContent {
                tree_sha,
                parent_sha,
                author_name,
                author_email,
                committer,
                committer_email,
                message,
                timestamp,
            }))
        }
        _ => bail!(format!("unsupported type: {}", content_type)),
    }
}

fn write_object(meta_data: &[u8], content: &[u8]) -> Result<String> {
    let mut hasher = Sha1::new();

    hasher.update(&meta_data);
    hasher.update(&content);

    let hash_object = format!("{:x}", hasher.finalize());
    let (dir_path, hash) = get_path_from_hash(&hash_object);
    let full_dir_path = format!("{}/{}", GIT_OBJECT_PATH, dir_path);
    let full_path = format!("{}/{}", full_dir_path, hash);

    if std::path::Path::new(&full_path).exists() {
        return Ok(hash_object);
    }

    fs::create_dir_all(&full_dir_path)
        .context(format!("creating directory {} failed", &full_dir_path))?;

    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&full_path)
        .context(format!("opening {} failed", &full_path))?;

    let buf_writer = BufWriter::new(file);
    let mut zlib_encoder = flate2::write::ZlibEncoder::new(buf_writer, Compression::default());

    let _ = zlib_encoder.write(meta_data);
    let _ = zlib_encoder.write(content);

    Ok(hash_object)
}

fn write_object_hash(object_hash_type: ObjectHashTypes) -> Result<String> {
    let (write_metadata, write_content) = match object_hash_type {
        ObjectHashTypes::Blob(content) => {
            let meta_data = format!("blob {}\0", content.len());
            (meta_data.as_bytes().to_vec(), content.as_bytes().to_vec())
        }
        ObjectHashTypes::Tree(items) => {
            let mut content = Vec::new();
            for tree_node in items {
                content.extend(tree_node.encode()?);
            }
            let meta_data = format!("tree {}\0", content.len());
            (meta_data.as_bytes().to_vec(), content)
        }
        ObjectHashTypes::Commit(commit_data) => {
            let commit_content = commit_data.to_string();
            let meta_data = format!("commit {}\0", commit_content.len());

            (
                meta_data.as_bytes().to_vec(),
                commit_content.as_bytes().to_vec(),
            )
        }
    };

    return write_object(&write_metadata, &write_content);
}

//@Performance: this is really slow, imagine hashing the whole content again and again
fn get_tree_nodes_from_git_directory(path: &std::path::Path) -> Result<Vec<TreeNode>> {
    const IGNORED_DIRECTORY: [&str; 2] = [".git", "target"];
    let mut tree_nodes = Vec::new();

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;

        if metadata.is_file() {
            let content = String::from_utf8(
                fs::read(entry.path()).context(format!("failed reading {:?}", &entry.path()))?,
            )
            .context(format!("failed parsing {:?} to string", entry.path()))?;
            let hash_object = write_object_hash(ObjectHashTypes::Blob(content))?;
            let git_mode = if metadata.mode() & 0o111 != 0 {
                100755
            } else {
                100644
            }; // some
            // magic for getting the mode number, don't ask me why
            tree_nodes.push(TreeNode {
                name: entry.file_name().to_string_lossy().to_string(),
                hash: hash_object,
                mode: git_mode,
            });
        } else if !IGNORED_DIRECTORY.contains(&entry.file_name().to_str().with_context(
            || "failed to get file name when checking whether directory should be ignored",
        )?) {
            let sub_tree_nodes = get_tree_nodes_from_git_directory(&entry.path())?;
            let hash_object = write_object_hash(ObjectHashTypes::Tree(sub_tree_nodes))?;
            tree_nodes.push(TreeNode {
                name: entry.file_name().to_string_lossy().to_string(),
                hash: hash_object,
                mode: 40000,
            });
        }
    }

    tree_nodes.sort_by(|a, b| a.name.cmp(&b.name));
    return Ok(tree_nodes);
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
        Commands::LsTree { hash } => {
            let object_hash = parse_object_hash(&hash)?;
            match object_hash {
                ObjectHashTypes::Tree(_) => println!("{}", object_hash),
                _ => println!("fatal: not a tree object"),
            }
        }
        Commands::WriteTree => {
            let path = env::current_dir()?;
            let tree_nodes = get_tree_nodes_from_git_directory(&path)?;

            let hash = write_object_hash(ObjectHashTypes::Tree(tree_nodes))?;
            println!("written object hash: {}", hash);
        }
        Commands::CommitTree {
            tree_sha,
            parent_sha,
            message,
        } => {
            let (author_name, author_email, committer, committer_email) = (
                get_commit_author_name(),
                get_commit_author_email(),
                get_commit_comitter_name(),
                get_commit_comitter_email(),
            );

            let timestamp = Utc::now().fixed_offset();
            let commit_content = CommitContent {
                tree_sha,
                parent_sha,
                author_name,
                author_email,
                committer,
                committer_email,
                message,
                timestamp,
            };

            let object_hash = write_object_hash(ObjectHashTypes::Commit(commit_content))?;
            println!("written object hash: {}", object_hash);
        }
    }
    Ok(())
}
