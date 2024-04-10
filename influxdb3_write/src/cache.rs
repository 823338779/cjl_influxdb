use crate::persister::serialize_to_parquet;
use crate::persister::Error;
use crate::ParquetFile;
use bytes::Bytes;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::physical_plan::SendableRecordBatchStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

type MetaData = Mutex<HashMap<String, HashMap<String, HashMap<String, ParquetFile>>>>;

#[derive(Debug)]
pub struct ParquetCache {
    object_store: Arc<dyn ObjectStore>,
    meta_data: MetaData,
    mem_pool: Arc<dyn MemoryPool>,
}

impl ParquetCache {
    // Create a new ParquetCache
    pub fn new(mem_pool: &Arc<dyn MemoryPool>) -> Self {
        Self {
            object_store: Arc::new(InMemory::new()),
            meta_data: Mutex::new(HashMap::new()),
            mem_pool: Arc::clone(mem_pool),
        }
    }

    // Get the parquet file metadata for a given database and table
    pub fn get_parquet_files(&self, database_name: &str, table_name: &str) -> Vec<ParquetFile> {
        self.meta_data
            .lock()
            .get(database_name)
            .and_then(|db| db.get(table_name))
            .cloned()
            .unwrap_or_default()
            .into_values()
            .collect()
    }

    // Persist a new parquet file to the cache or pass an object store path to update a currently
    // existing file in the cache
    pub async fn persist_parquet_file(
        &self,
        db_name: &str,
        table_name: &str,
        min_time: i64,
        max_time: i64,
        record_batches: SendableRecordBatchStream,
        path: Option<ObjPath>,
    ) -> Result<(), Error> {
        let parquet = serialize_to_parquet(Arc::clone(&self.mem_pool), record_batches).await?;
        // Generate a path for this
        let id = uuid::Uuid::new_v4();
        let parquet_path =
            path.unwrap_or_else(|| ObjPath::from(format!("{db_name}-{table_name}-{id}")));
        let size_bytes = parquet.bytes.len() as u64;
        let meta_data = parquet.meta_data;

        self.object_store.put(&parquet_path, parquet.bytes).await?;

        let path = parquet_path.to_string();
        let parquet_files = || -> HashMap<String, ParquetFile> {
            HashMap::from([(
                path.clone(),
                ParquetFile {
                    path: path.clone(),
                    size_bytes,
                    row_count: meta_data.num_rows as u64,
                    min_time,
                    max_time,
                },
            )])
        };
        self.meta_data
            .lock()
            .entry(db_name.into())
            .and_modify(|db| {
                db.entry(table_name.into())
                    .and_modify(|files| {
                        files.insert(
                            path.clone(),
                            ParquetFile {
                                path: path.clone(),
                                size_bytes,
                                row_count: meta_data.num_rows as u64,
                                min_time,
                                max_time,
                            },
                        );
                    })
                    .or_insert_with(parquet_files);
            })
            .or_insert_with(|| HashMap::from([(table_name.into(), parquet_files())]));

        Ok(())
    }

    // Load the file from the cache
    pub async fn load_parquet_file(&self, path: ObjPath) -> Result<Bytes, Error> {
        Ok(self.object_store.get(&path).await?.bytes().await?)
    }

    /// Remove the file from the cache
    pub async fn remove_parquet_file(&self, path: ObjPath) -> Result<(), Error> {
        self.object_store.delete(&path).await?;
        let mut split = path.as_ref().split('-');
        let db = split
            .next()
            .expect("cache keys are in the form db-table-uuid");
        let table = split
            .next()
            .expect("cache keys are in the form db-table-uuid");
        self.meta_data
            .lock()
            .get_mut(db)
            .and_then(|tables| tables.get_mut(table))
            .expect("the file exists in the meta_data table as well")
            .remove(path.as_ref());

        Ok(())
    }

    // Get a reference to the ObjectStore backing the cache
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.object_store)
    }
}
