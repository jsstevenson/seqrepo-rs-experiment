use sqlx::sqlite::SqlitePoolOptions;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct SeqRepoImportError;

#[derive(Debug, Clone)]
struct SeqRepoInstanceError;

fn validate_seqrepo_instance(seqrepo_instance: &PathBuf) -> Result<(), SeqRepoInstanceError> {
    let pathbuf_binding = seqrepo_instance.clone();
    let path = pathbuf_binding.as_path();
    let aliases = path.join("aliases.sqlite3");
    if !aliases.exists() || !aliases.is_file() {
        return Err(SeqRepoInstanceError);
    }

    // Check the sequences directory exists
    let sequences = path.join("sequences");
    if !sequences.exists() || !sequences.is_dir() {
        return Err(SeqRepoInstanceError);
    }

    // Check the db.sqlite3 file exists inside sequences.
    let db_file = sequences.join("db.sqlite3");
    if !db_file.exists() || !db_file.is_file() {
        return Err(SeqRepoInstanceError);
    }

    Ok(())
}

pub async fn import(seqrepo_instance: &PathBuf) -> Result<(), SeqRepoImportError> {
    println!("sdlf;kjd");
    validate_seqrepo_instance(seqrepo_instance).map_err(|_| SeqRepoImportError)?;
    Ok(())
}
