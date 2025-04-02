use futures::StreamExt;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Error, SqlitePool};
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

#[derive(sqlx::FromRow, Debug)]
struct SeqAlias {
    seq_id: String,
    namespace: String,
    alias: String,
    added: String,
    is_current: bool,
}

async fn get_sqlite_connection(db: &PathBuf) -> Result<SqlitePool, Error> {
    let opts = SqliteConnectOptions::new().filename(db).read_only(true);
    Ok(SqlitePool::connect_with(opts).await?)
}

async fn import_seqalias_db(seqalias_db: &PathBuf) -> Result<(), Error> {
    let pool = get_sqlite_connection(seqalias_db).await.unwrap();
    let mut stream = sqlx::query_as::<_, SeqAlias>(
        "SELECT seq_id, namespace, alias, added, is_current FROM seqalias;",
    )
    .fetch(&pool);

    while let Some(row) = stream.next().await {
        match row {
            Ok(seq_alias) => {
                //println!("Got seq_alias: {:?}", seq_alias.seq_id);
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    Ok(())
}

#[derive(sqlx::FromRow, Debug)]
struct FastadirEntry {
    seq_id: String,
    len: u64,
    alpha: String,
    added: String,
    relpath: String,
}

async fn import_fastadir(fasta_db: &PathBuf) -> Result<(), Error> {
    let pool = get_sqlite_connection(fasta_db).await.unwrap();
    let mut stream = sqlx::query_as::<_, FastadirEntry>(
        "SELECT seq_id, len, alpha, added, relpath FROM seqinfo;",
    )
    .fetch(&pool);

    while let Some(row) = stream.next().await {
        match row {
            Ok(fastadir_entry) => {
                //println!("Got fastadir row: {:?}", fastadir_entry);
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok(())
}

pub async fn import(seqrepo_instance: &PathBuf) -> Result<(), SeqRepoImportError> {
    validate_seqrepo_instance(seqrepo_instance).map_err(|_| SeqRepoImportError)?;

    let seqalias_db = seqrepo_instance.clone().join("aliases.sqlite3");
    import_seqalias_db(&seqalias_db)
        .await
        .map_err(|_| SeqRepoImportError)?;
    let fastadir_db = seqrepo_instance.clone().join("sequences").join("db.sqlite3");
    import_fastadir(&fastadir_db)
        .await
        .map_err(|_| SeqRepoImportError)?;
    Ok(())
}
