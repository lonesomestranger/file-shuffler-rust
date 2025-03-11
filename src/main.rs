use dialoguer::{Select, theme::ColorfulTheme};
use rand::prelude::*;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write, stdin};
use std::path::{Path, PathBuf};
use std::time::Instant;

const CHUNK_SIZE: usize = 512 * 1024 * 1024;
const BUF_CAPACITY: usize = 8 * 1024 * 1024;

fn main() -> io::Result<()> {
    let mut exe_path = std::env::current_exe()?;
    exe_path.pop();
    let current_dir = exe_path;

    let input_file = select_file(&current_dir, "Select input file").unwrap_or_else(|| {
        println!("No file selected, exiting.");
        std::process::exit(1);
    });

    let output_file = format!(
        "shuffled_{}",
        input_file.file_name().unwrap().to_str().unwrap()
    );
    let temp_dir = "temp_chunks";

    let total_time_start = Instant::now();
    std::fs::create_dir_all(temp_dir)?;
    let mut chunk_count = 0;
    {
        let mut reader = BufReader::with_capacity(BUF_CAPACITY, File::open(&input_file)?);
        let mut current_chunk = Vec::new();
        let mut current_chunk_size = 0;
        let mut line = String::new();

        while reader.read_line(&mut line)? > 0 {
            current_chunk_size += line.len();
            current_chunk.push(line.clone());

            if current_chunk_size >= CHUNK_SIZE {
                write_chunk(&current_chunk, temp_dir, chunk_count)?;
                chunk_count += 1;
                current_chunk.clear();
                current_chunk_size = 0;
            }
            line.clear();
        }

        if !current_chunk.is_empty() {
            write_chunk(&current_chunk, temp_dir, chunk_count)?;
            chunk_count += 1;
        }
    }

    merge_chunks(temp_dir, chunk_count, output_file)?;

    std::fs::remove_dir_all(temp_dir)?;

    let total_time_end = total_time_start.elapsed();
    println!("Total elapsed time {:?}", total_time_end);

    println!("Press Enter to exit...");
    let mut _dummy = String::new();
    stdin().read_line(&mut _dummy)?;
    Ok(())
}

fn write_chunk(chunk: &[String], temp_dir: &str, chunk_index: usize) -> io::Result<()> {
    let mut rng = thread_rng();
    let chunk_path = format!("{}/chunk_{}.txt", temp_dir, chunk_index);
    let mut writer = BufWriter::with_capacity(BUF_CAPACITY, File::create(chunk_path)?);
    let mut indices: Vec<usize> = (0..chunk.len()).collect();

    let shuffle_start_time = Instant::now();
    indices.shuffle(&mut rng);
    let shuffle_duration = shuffle_start_time.elapsed();
    println!(
        "Shuffling chunk {} took: {:?}",
        chunk_index + 1,
        shuffle_duration
    );

    let mut buffer = Vec::with_capacity(BUF_CAPACITY);
    for &index in &indices {
        buffer.extend_from_slice(chunk[index].as_bytes());
        if buffer.len() >= BUF_CAPACITY {
            writer.write_all(&buffer)?;
            buffer.clear();
        }
    }
    if !buffer.is_empty() {
        writer.write_all(&buffer)?;
    }
    writer.flush()?;

    Ok(())
}
fn merge_chunks(temp_dir: &str, chunk_count: usize, output_file: String) -> io::Result<()> {
    let mut readers: Vec<BufReader<File>> = (0..chunk_count)
        .map(|i| {
            let chunk_path = format!("{}/chunk_{}.txt", temp_dir, i);
            BufReader::with_capacity(BUF_CAPACITY, File::open(chunk_path).unwrap())
        })
        .collect();

    let mut lines: Vec<Option<String>> = vec![Some(String::new()); chunk_count];
    let mut rng = thread_rng();

    for (i, reader) in readers.iter_mut().enumerate() {
        if let Some(line) = lines[i].as_mut() {
            if reader.read_line(line)? == 0 {
                lines[i] = None;
            }
        }
    }

    let mut writer = BufWriter::with_capacity(BUF_CAPACITY, File::create(output_file)?);
    let mut buffer = Vec::with_capacity(BUF_CAPACITY);

    let mut valid_indices: Vec<usize> = (0..chunk_count).collect();

    while valid_indices.len() > 0 {
        valid_indices.retain(|&i| lines[i].is_some());
        if valid_indices.is_empty() {
            break;
        }

        let chosen_index = valid_indices[rng.gen_range(0..valid_indices.len())];

        if let Some(line) = lines[chosen_index].take() {
            buffer.extend_from_slice(line.as_bytes());
            if buffer.len() >= BUF_CAPACITY {
                writer.write_all(&buffer)?;
                buffer.clear();
            }

            let mut next_line = String::new();
            if readers[chosen_index].read_line(&mut next_line)? > 0 {
                lines[chosen_index] = Some(next_line);
            } else {
                lines[chosen_index] = None;
            }
        }
    }
    if !buffer.is_empty() {
        writer.write_all(&buffer)?;
    }
    writer.flush()?;
    Ok(())
}

fn select_file(initial_dir: &Path, prompt: &str) -> Option<PathBuf> {
    let mut entries = Vec::new();

    if let Ok(read_dir) = std::fs::read_dir(initial_dir) {
        for entry in read_dir {
            if let Ok(entry) = entry {
                if entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
                    entries.push(entry.path());
                }
            }
        }
    }

    if entries.is_empty() {
        println!("No files found in the current directory.");
        return None;
    }
    let items: Vec<String> = entries
        .iter()
        .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .default(0)
        .items(&items)
        .interact_opt()
        .unwrap_or(None);

    selection.map(|index| entries[index].clone())
}
