use bytes::{Buf, BytesMut};
use crc32fast::Hasher;
use im::HashMap;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

const B3_DATA_FILE_EXTENSION: &str = "b3data";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error")]
    IO(#[from] std::io::Error),
    #[error("could not encode to bincode")]
    BincodeEncode(#[from] bincode::error::EncodeError),
    #[error("could not decode from bincode to type")]
    BincodeDecode(#[from] bincode::error::DecodeError),
    #[error("error getting system time")]
    Time(#[from] std::time::SystemTimeError),
    #[error("corrupt data for entry, failed CRC32 check")]
    CorruptEntryData {
        entry_file_path: PathBuf,
        entry_position: u32,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Default, PartialEq)]
pub enum SyncStrategy {
    None,
    #[default]
    FullSync,
}

#[derive(Default)]
pub struct Options {
    sync_strategy: SyncStrategy,
}

#[derive(Clone)]
pub struct Db<K, V> {
    db_impl: Arc<DbImpl<K, V>>,
}

impl<K, V> Db<K, V>
where
    K: Clone + Serialize + DeserializeOwned + Eq + std::hash::Hash,
    V: Serialize + DeserializeOwned,
{
    pub async fn new(dir: PathBuf, options: Options) -> Result<Self> {
        let db_impl = DbImpl::new(dir, options).await?;

        Ok(Self {
            db_impl: Arc::new(db_impl),
        })
    }

    pub async fn insert(&self, k: K, v: V) -> Result<()> {
        self.db_impl.insert(k, &v).await
    }

    pub async fn get<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        Q: ?Sized,
        K: Borrow<Q>,
        Q: std::hash::Hash + Eq,
    {
        self.db_impl.get(k).await
    }

    pub async fn delete<Q>(&self, k: &Q) -> Result<()>
    where
        Q: ?Sized,
        K: Borrow<Q>,
        Q: std::hash::Hash + Eq + Serialize,
    {
        self.db_impl.delete(k).await
    }

    pub async fn keys(&self) -> Vec<K> {
        self.db_impl.keys().await
    }
}

#[derive(Clone, Copy, Debug)]
struct Mapping {
    file_id: u32,
    entry_size: u32,
    entry_position: u32,
    _timestamp: u128,
}

impl Mapping {
    fn file_path(&self, dir: &Path) -> PathBuf {
        let mut file_path = dir.to_path_buf();
        file_path.push(self.file_id.to_string());
        file_path.with_added_extension(B3_DATA_FILE_EXTENSION)
    }
}

// TODO what should the on-disk representation of this be?
#[derive(Serialize, Deserialize)]
enum InsertOrDelete<V> {
    Insert(V),
    #[serde(rename = "__b3_tombstone")]
    Tombstone,
}

struct DbImpl<K, V> {
    dir: PathBuf,
    options: Options,
    locked_data: RwLock<WriterData<K>>,
    bincode_config: bincode::config::Configuration<
        bincode::config::BigEndian,
        bincode::config::Fixint,
        bincode::config::NoLimit,
    >,
    _v: PhantomData<V>,
}

struct WriterData<K> {
    keydir: HashMap<K, Mapping>,
    current_write_file: tokio::fs::File,
    current_file_id: u32,
    current_position: u32,
    /// the buffer into which we bincode the key and value.
    ///
    /// idea: a new type wrapper that has one method,
    /// that when called clears the buf,
    /// and yields a mutable reference to the underlying buf.
    /// this way you must clear the buf in order to use it
    serialization_buf: Vec<u8>,
}

impl<K, V> DbImpl<K, V>
where
    K: Clone + Serialize + DeserializeOwned + Eq + std::hash::Hash,
    V: Serialize + DeserializeOwned,
{
    async fn new(dir: PathBuf, options: Options) -> Result<Self> {
        let mut b3_data_file_glob = dir.to_string_lossy();
        let b3_data_file_glob = b3_data_file_glob.to_mut();
        b3_data_file_glob.push_str("*.b3data");

        let mut data_files = vec![];

        let mut entries = tokio::fs::read_dir(&dir).await?;

        // Iterate over the entries using a while let loop
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if path.is_file()
                && let Some(ext) = path.extension()
                && ext == B3_DATA_FILE_EXTENSION
            {
                data_files.push(path)
            }
        }

        let current_file_id = data_files
            .iter()
            .map(|path| {
                path.file_stem()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .parse::<u32>()
                    .unwrap()
            })
            .max()
            .map(|max| max + 1)
            .unwrap_or(1);

        data_files.sort();

        let mut keydir = HashMap::new();

        let bincode_config = bincode::config::standard()
            .with_big_endian()
            .with_fixed_int_encoding();

        for data_file in data_files {
            match Self::read_file_and_update_keydir(&data_file, &mut keydir, bincode_config).await {
                Ok(()) => (),
                Err(e) => match e {
                    Error::IO(error) => match error.kind() {
                        std::io::ErrorKind::UnexpectedEof => continue,
                        _kind => Err(error)?,
                    },
                    Error::BincodeEncode(_encode_error) => continue,
                    Error::BincodeDecode(_decode_error) => continue,
                    Error::CorruptEntryData { .. } => continue,
                    Error::Time(_system_time_error) => (),
                },
            }
        }

        let mut current_file_path = dir.clone();
        current_file_path.push(current_file_id.to_string());
        current_file_path.add_extension(B3_DATA_FILE_EXTENSION);

        let current_write_file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(current_file_path)
            .await?;

        Ok(Self {
            dir,
            options,
            locked_data: RwLock::new(WriterData {
                keydir,
                current_write_file,
                current_file_id,
                current_position: 0,
                serialization_buf: vec![],
            }),
            bincode_config,
            _v: PhantomData,
        })
    }

    async fn read_file_and_update_keydir(
        path: &Path,
        keydir: &mut HashMap<K, Mapping>,
        bincode_config: bincode::config::Configuration<
            bincode::config::BigEndian,
            bincode::config::Fixint,
            bincode::config::NoLimit,
        >,
    ) -> Result<()> {
        let mut f = tokio::fs::File::open(path).await?;

        let mut current_position: u32 = 0;

        let header_length = std::mem::size_of::<u32>()
            + std::mem::size_of::<u128>()
            + std::mem::size_of::<u32>()
            + std::mem::size_of::<u32>();

        let mut buf = BytesMut::new();

        loop {
            buf.resize(header_length, 0);

            f.read_exact(&mut buf).await?;

            let crc_from_disk = buf.get_u32();
            let millis_since_epoch = buf.get_u128();
            let key_size = buf.get_u32();
            let value_size = buf.get_u32();

            buf.resize(
                usize::try_from(key_size).unwrap() + usize::try_from(value_size).unwrap(),
                0,
            );

            f.read_exact(&mut buf).await?;

            let mut hasher = Hasher::new();
            hasher.update(&millis_since_epoch.to_be_bytes());
            hasher.update(&key_size.to_be_bytes());
            hasher.update(&value_size.to_be_bytes());
            hasher.update(&buf);
            let crc_calculated = hasher.finalize();

            if crc_from_disk != crc_calculated {
                return Err(Error::CorruptEntryData {
                    entry_file_path: path.to_path_buf(),
                    entry_position: current_position,
                });
            }

            let key_bytes = buf.split_to(usize::try_from(key_size).unwrap());
            let value_bytes = buf.split_to(usize::try_from(value_size).unwrap());

            let (k, _): (K, usize) = bincode::serde::decode_from_slice(&key_bytes, bincode_config)?;

            let (v, _): (InsertOrDelete<V>, usize) =
                bincode::serde::decode_from_slice(&value_bytes, bincode_config)?;

            match v {
                InsertOrDelete::Insert(_v) => {
                    let mapping = Mapping {
                        file_id: path.file_stem().unwrap().to_str().unwrap().parse().unwrap(),
                        entry_size: u32::try_from(header_length).unwrap() + key_size + value_size,
                        entry_position: current_position,
                        _timestamp: millis_since_epoch,
                    };

                    keydir.insert(k, mapping);
                }
                InsertOrDelete::Tombstone => {
                    keydir.remove(&k);
                }
            }

            current_position += u32::try_from(header_length).unwrap() + key_size + value_size;
        }
    }

    async fn insert(&self, k: K, v: &V) -> Result<()> {
        let mut guard = self.locked_data.write().await;

        // necessary to allow split borrow through rwlock
        let writer_data = guard.deref_mut();

        writer_data.serialization_buf.clear();

        let key_serialized_size = bincode::serde::encode_into_std_write(
            &k,
            &mut writer_data.serialization_buf,
            self.bincode_config,
        )?;

        let value_serialized_size = bincode::serde::encode_into_std_write(
            InsertOrDelete::Insert(v),
            &mut writer_data.serialization_buf,
            self.bincode_config,
        )?;

        let millis_since_epoch = {
            let now = std::time::SystemTime::now();
            now.duration_since(std::time::UNIX_EPOCH)?.as_millis()
        };

        let crc = {
            let mut hasher = crc32fast::Hasher::new();

            hasher.update(&millis_since_epoch.to_be_bytes());
            hasher.update(
                &u32::try_from(key_serialized_size)
                    .expect("key size must be <= u32::MAX")
                    .to_be_bytes(),
            );
            hasher.update(
                &u32::try_from(value_serialized_size)
                    .expect("key size must be <= u32::MAX")
                    .to_be_bytes(),
            );
            hasher.update(&writer_data.serialization_buf);

            hasher.finalize()
        };

        writer_data.current_write_file.write_u32(crc).await?;
        writer_data
            .current_write_file
            .write_u128(millis_since_epoch)
            .await?;
        writer_data
            .current_write_file
            .write_u32(u32::try_from(key_serialized_size).expect("key size must be <= u32::MAX"))
            .await?;
        writer_data
            .current_write_file
            .write_u32(
                u32::try_from(value_serialized_size).expect("value size must be <= u32::MAX"),
            )
            .await?;
        writer_data
            .current_write_file
            .write_all(&writer_data.serialization_buf)
            .await?;

        if self.options.sync_strategy == SyncStrategy::FullSync {
            writer_data.current_write_file.sync_all().await?;
        }

        let entry_size: u32 = (std::mem::size_of::<u32>()
            + std::mem::size_of::<u128>()
            + std::mem::size_of::<u32>()
            + std::mem::size_of::<u32>()
            + writer_data.serialization_buf.len())
        .try_into()
        .expect("entry size must be <= u32::MAX bytes");

        writer_data.keydir.insert(
            k,
            Mapping {
                file_id: writer_data.current_file_id,
                entry_size,
                entry_position: writer_data.current_position,
                _timestamp: millis_since_epoch,
            },
        );

        guard.current_position += entry_size;

        Ok(())
    }

    async fn get<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        Q: ?Sized,
        K: Borrow<Q>,
        Q: std::hash::Hash + Eq,
    {
        // super duper fast, since all we are doing is
        // cloning an im::HashMap, which is basically a
        // pointer clone and refcount increment
        let keydir = {
            let lock = self.locked_data.read().await;
            lock.keydir.clone()
        };

        // TODO figure out how to solve reading of stale data during merges
        //
        // from this point on, once the keydir has been cloned,
        // it is *entirely* uncoordinated from any
        // other database operations at this point.
        // this is because inserts and deletes are append-only.
        // this means that, at worst, this function can read stale, valid data.
        //
        // merges are different, though, since merging
        // involves rewriting the database files.
        //
        // the following sequence of operations can cause trouble.
        //
        // - this operation clones the keydir but has not yet read bytes from disk
        // - a merge operation is initiated
        // - this operation then then goes to read bytes from disk
        //
        // this could result in:
        // - an IO error: file no longer exists, etc.
        // - a deserialization error: failure to deserialize to a well-formed instance of V
        // - the most concerning, a non-error: a deserialization to a well-formed
        //   but *incorrect* instance of V
        //
        // for this reason it's probably best to perform a merge:
        // - at startup, before any readers are allowed to read
        // - at shutdown, once all readers have drained.
        if let Some(mapping) = keydir.get(k) {
            let read_file_path = mapping.file_path(&self.dir);

            let mut read_file = tokio::fs::File::open(&read_file_path).await?;

            let mut buf = BytesMut::new();
            buf.resize(usize::try_from(mapping.entry_size).unwrap(), 0);

            read_file.read_exact(&mut buf).await?;

            let crc = buf.get_u32();
            let crc_from_disk = crc32fast::hash(&buf);

            if crc != crc_from_disk {
                return Err(Error::CorruptEntryData {
                    entry_file_path: read_file_path,
                    entry_position: mapping.entry_position,
                });
            }

            let _millis_since_epoch = buf.get_u128();
            let key_size = buf.get_u32();
            let value_size = buf.get_u32();

            // unused on a simple `get`, but used when we load
            // files from disk at startup
            let _key_bytes = buf.split_to(usize::try_from(key_size).unwrap());

            let value_bytes = buf.split_to(usize::try_from(value_size).unwrap());

            let (v, _): (InsertOrDelete<V>, usize) =
                bincode::serde::decode_from_slice(&value_bytes, self.bincode_config)?;

            match v {
                InsertOrDelete::Insert(v) => Ok(Some(v)),
                InsertOrDelete::Tombstone => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    async fn delete<Q>(&self, k: &Q) -> Result<()>
    where
        Q: ?Sized,
        K: Borrow<Q>,
        Q: std::hash::Hash + Eq + Serialize,
    {
        // only actually insert a physical delete record
        // if we know about the key in the keydir
        let keydir = { self.locked_data.read().await.keydir.clone() };

        if keydir.contains_key(k) {
            let mut guard = self.locked_data.write().await;

            let writer_data = guard.deref_mut();

            writer_data.serialization_buf.clear();

            let key_serialized_size = bincode::serde::encode_into_std_write(
                k,
                &mut writer_data.serialization_buf,
                self.bincode_config,
            )?;

            let value_serialized_size = bincode::serde::encode_into_std_write(
                InsertOrDelete::Tombstone::<V>,
                &mut writer_data.serialization_buf,
                self.bincode_config,
            )?;

            let millis_since_epoch = {
                let now = std::time::SystemTime::now();
                now.duration_since(std::time::UNIX_EPOCH)?.as_millis()
            };

            let crc = {
                let mut hasher = crc32fast::Hasher::new();

                hasher.update(&millis_since_epoch.to_be_bytes());
                hasher.update(
                    &u32::try_from(key_serialized_size)
                        .expect("key size must be <= u32::MAX")
                        .to_be_bytes(),
                );
                hasher.update(
                    &u32::try_from(value_serialized_size)
                        .expect("key size must be <= u32::MAX")
                        .to_be_bytes(),
                );
                hasher.update(&writer_data.serialization_buf);

                hasher.finalize()
            };

            writer_data.current_write_file.write_u32(crc).await?;
            writer_data
                .current_write_file
                .write_u128(millis_since_epoch)
                .await?;
            writer_data
                .current_write_file
                .write_u32(
                    u32::try_from(key_serialized_size).expect("key size must be <= u32::MAX"),
                )
                .await?;
            writer_data
                .current_write_file
                .write_u32(
                    u32::try_from(value_serialized_size).expect("value size must be <= u32::MAX"),
                )
                .await?;
            writer_data
                .current_write_file
                .write_all(&writer_data.serialization_buf)
                .await?;

            if self.options.sync_strategy == SyncStrategy::FullSync {
                writer_data.current_write_file.sync_all().await?;
            }

            let entry_size: u32 = (std::mem::size_of::<u32>()
                + std::mem::size_of::<u128>()
                + std::mem::size_of::<u32>()
                + std::mem::size_of::<u32>()
                + writer_data.serialization_buf.len())
            .try_into()
            .expect("entry size must be <= u32::MAX bytes");

            writer_data.keydir.remove(k);

            writer_data.current_position += entry_size;

            Ok(())
        } else {
            Ok(())
        }
    }
}

impl<K, V> DbImpl<K, V>
where
    K: Clone,
{
    async fn keys(&self) -> Vec<K> {
        let keydir = {
            let locked = self.locked_data.read().await;
            locked.keydir.clone()
        };

        keydir.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn insert_and_get() {
        let dir = temp_dir::TempDir::with_prefix("b3").unwrap();
        let db: Db<String, String> = Db::new(dir.path().to_owned(), Options::default())
            .await
            .unwrap();

        db.insert("hello".to_string(), "there".to_string())
            .await
            .unwrap();

        let expected = db.get("hello").await.unwrap();

        assert_eq!(expected, Some("there".to_string()));
    }

    #[tokio::test]
    async fn insert_and_get_and_delete_and_get() {
        let dir = temp_dir::TempDir::with_prefix("b3").unwrap();
        let db: Db<String, String> = Db::new(dir.path().to_owned(), Options::default())
            .await
            .unwrap();

        db.insert("hello".to_string(), "there".to_string())
            .await
            .unwrap();

        let expected = db.get("hello").await.unwrap();

        assert_eq!(expected, Some("there".to_string()));

        db.delete("hello").await.unwrap();

        let expected = db.get("hello").await.unwrap();

        assert_eq!(expected, None);
    }

    #[tokio::test]
    async fn get_with_no_prior_insert() {
        let dir = temp_dir::TempDir::with_prefix("b3").unwrap();
        let db: Db<String, String> = Db::new(dir.path().to_owned(), Options::default())
            .await
            .unwrap();

        let expected = db.get("nothing_here").await.unwrap();

        assert!(expected.is_none());
    }

    #[tokio::test]
    async fn delete_with_no_prior_insert() {
        let dir = temp_dir::TempDir::with_prefix("b3").unwrap();
        let db: Db<String, String> = Db::new(dir.path().to_owned(), Options::default())
            .await
            .unwrap();

        db.delete("not here").await.unwrap();

        let expected = db.get("not here").await.unwrap();

        assert_eq!(expected, None)
    }

    #[tokio::test]
    async fn delete_followed_by_insert() {
        let dir = temp_dir::TempDir::with_prefix("b3").unwrap();
        let db: Db<String, String> = Db::new(dir.path().to_owned(), Options::default())
            .await
            .unwrap();

        db.delete("not here").await.unwrap();

        db.insert("not here".to_string(), "actually".to_string())
            .await
            .unwrap();

        let expected = db.get("not here").await.unwrap().unwrap();

        assert_eq!(expected, "actually");
    }

    #[tokio::test]
    async fn loads_files_in_order() {
        let dir = temp_dir::TempDir::with_prefix("b3").unwrap();

        // create db1
        // insert 1 record
        {
            let db: Db<String, String> = Db::new(dir.path().to_owned(), Options::default())
                .await
                .unwrap();

            db.insert("hello".to_string(), "there".to_string())
                .await
                .unwrap();

            let expected = db.get("hello").await.unwrap().unwrap();

            assert_eq!(expected, "there".to_string());
        }

        // create db2
        // assert that previously inserted record is loaded from disk
        // delete that record
        {
            let db: Db<String, String> = Db::new(dir.path().to_owned(), Options::default())
                .await
                .unwrap();

            let expected = db.get("hello").await.unwrap().unwrap();

            assert_eq!(expected, "there".to_string());

            db.delete("hello").await.unwrap();
        }

        // create db3
        // assert previously inserted and then deleted record does not exist
        {
            let db: Db<String, String> = Db::new(dir.path().to_owned(), Options::default())
                .await
                .unwrap();

            let expected = db.get("hello").await.unwrap();

            assert!(expected.is_none());
        }
    }

    #[tokio::test]
    async fn keys() {
        let dir = temp_dir::TempDir::with_prefix("b3").unwrap();

        let db: Db<String, String> = Db::new(dir.path().to_owned(), Options::default())
            .await
            .unwrap();

        let keys1 = db.keys().await;

        assert_eq!(keys1, Vec::<String>::new());

        db.insert("hello".to_string(), "there".to_string())
            .await
            .unwrap();

        let keys2 = db.keys().await;

        assert_eq!(keys2, vec!["hello"]);

        db.insert("hi".to_string(), "ok".to_string()).await.unwrap();

        let mut keys2 = db.keys().await;

        keys2.sort();

        assert_eq!(keys2, vec!["hello", "hi"]);
    }
}
