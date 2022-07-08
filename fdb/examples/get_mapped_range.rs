use bytes::Bytes;

use fdb::database::FdbDatabase;
use fdb::error::FdbResult;
use fdb::range::{Range, RangeOptions};
use fdb::transaction::{FdbTransaction, Transaction};
use fdb::tuple::Tuple;
use fdb::{Key, Mapper, Value};

use tokio::runtime::Runtime;
use tokio_stream::StreamExt;

use std::env;
use std::error::Error;

// In Record Layer, there is a class `FDBRecordStoreKeyspace` that
// describes various spaces such as `STORE_INFO`, `RECORD`, `INDEX`,
// `INDEX_SECONDARY_SPACE`. etc.,
//
// "prefix" can be the subspace of the record store.
//
// ```
// ("prefix", "INDEX", "index-key-of-record-00012345", "primary-key-of-record-00012345") = ()
// ```
//
// Stores index
//
// ```
// ("prefix", "RECORD", "primary-key-of-record-00012345", 0) = ("data-of-record-00012345", 0)
// ("prefix", "RECORD", "primary-key-of-record-00012345", 1) = ("data-of-record-00012345", 1)
// ("prefix", "RECORD", "primary-key-of-record-00012345", 2) = ("data-of-record-00012345", 2)
// ```
//
// Stores record. Record key prefix excludes the "split" part, while
// record key includes the split part.

const PREFIX: &str = "prefix";
const RECORD: &str = "RECORD";
const INDEX: &str = "INDEX";

fn empty() -> Bytes {
    Tuple::new().pack()
}

fn primary_key(i: u32) -> String {
    format!("primary-key-of-record-{:08}", i)
}

fn index_key(i: u32) -> String {
    format!("index-key-of-record-{:08}", i)
}

fn data_of_record(i: u32) -> String {
    format!("data-of-record-{:08}", i)
}

fn mapper() -> Mapper {
    let mapper_tup: (&'static str, &'static str, &'static str, &'static str) =
        (PREFIX, RECORD, "{K[3]}", "{...}");

    let mapper = {
        let mut tup = Tuple::new();

        // PREFIX
        tup.add_string((mapper_tup.0).to_string());

        // RECORD
        tup.add_string((mapper_tup.1).to_string());

        // "{K[3]}"
        tup.add_string((mapper_tup.2).to_string());

        // "{...}"
        tup.add_string((mapper_tup.3).to_string());

        tup
    };

    mapper.into()
}

const SPLIT_SIZE: u32 = 3;

// Example
//
// ```
// ("prefix", "INDEX", "index-key-of-record-00012345", "primary-key-of-record-00012345")
// ```
fn index_entry_key(i: u32) -> Key {
    let index_tup: (&'static str, &'static str, String, String) =
        (PREFIX, INDEX, index_key(i), primary_key(i));

    let index_key = {
        let mut tup = Tuple::new();

        // PREFIX
        tup.add_string((index_tup.0).to_string());

        // INDEX
        tup.add_string((index_tup.1).to_string());

        tup.add_string(index_tup.2);

        tup.add_string(index_tup.3);

        tup
    };

    index_key.pack().into()
}

// Example
//
// ```
// ("prefix", "RECORD", "primary-key-of-record-00012345")
// ```
//
// Is an example of record key prefix, without split. We don't use it
// in our example, but it is used in Java integration test.
#[allow(dead_code)]
fn record_key_prefix(i: u32) -> Tuple {
    let rec_key_prefix_tup: (&'static str, &'static str, String) = (PREFIX, RECORD, primary_key(i));

    {
        let mut tup = Tuple::new();

        // PREFIX
        tup.add_string((rec_key_prefix_tup.0).to_string());

        // RECORD
        tup.add_string((rec_key_prefix_tup.1).to_string());

        tup.add_string(rec_key_prefix_tup.2);

        tup
    }
}

// Example (contains split)
//
// ```
// ("prefix", "RECORD", "primary-key-of-record-00012345", 2)
// ```
fn record_key(i: u32, split: u32) -> Key {
    let rec_key_tup: (&'static str, &'static str, String, u32) =
        (PREFIX, RECORD, primary_key(i), split);

    let rec_key = {
        let mut tup = Tuple::new();

        // PREFIX
        tup.add_string((rec_key_tup.0).to_string());

        // RECORD
        tup.add_string((rec_key_tup.1).to_string());

        tup.add_string(rec_key_tup.2);

        tup.add_i64(rec_key_tup.3.into());

        tup
    };

    rec_key.pack().into()
}

// Example (contains split)
//
// ```
// ("data-of-record-00012345", 2)
// ```
fn record_value(i: u32, split: u32) -> Value {
    let rec_value_tup: (String, u32) = (data_of_record(i), split);

    let rec_value = {
        let mut tup = Tuple::new();

        tup.add_string(rec_value_tup.0);

        tup.add_i64(rec_value_tup.1.into());

        tup
    };

    rec_value.pack().into()
}

fn insert_record_with_index(tr: &FdbTransaction, i: u32) {
    tr.set(index_entry_key(i), empty());

    (0..SPLIT_SIZE).for_each(|j| {
        tr.set(record_key(i, j), record_value(i, j));
    });
}

async fn insert_record_with_indexes(n: u32, db: &FdbDatabase) -> FdbResult<()> {
    db.run(|tr| async move {
        (0..n).for_each(|i| insert_record_with_index(&tr, i));
        Ok(())
    })
    .await
}

fn main() -> Result<(), Box<dyn Error>> {
    let fdb_cluster_file = env::var("FDB_CLUSTER_FILE").expect("FDB_CLUSTER_FILE not defined!");
    unsafe {
        // Assume this to be atleast `710`
        fdb::select_api_version(fdb::FDB_API_VERSION as i32);
        fdb::start_network();
    }

    let fdb_database = fdb::open_database(fdb_cluster_file)?;

    let rt = Runtime::new()?;

    let cloned_fdb_database = fdb_database.clone();

    rt.block_on(async {
        let fdb_database = cloned_fdb_database;

        // Clear the database.
        fdb_database
            .run(|tr| async move {
                tr.clear_range(Range::new(Bytes::new(), Bytes::from_static(b"\xFF")));

                Ok(())
            })
            .await?;

        // Records with primary key of 0..=4 will be inserted.
        insert_record_with_indexes(5, &fdb_database).await?;

        fdb_database
            .run(|tr| async move {
                // Get mapped key values from 1..=3.
                let mut mapped_range_stream = Range::new(index_entry_key(1), index_entry_key(4))
                    .into_mapped_stream(&tr, mapper(), RangeOptions::default());

                while let Some(x) = mapped_range_stream.next().await {
                    let (kv, mapped_range, mapped_kvs) = x?.into_parts();

                    println!();
                    println!("-----");

                    let (kv_key, kv_value) = kv.into_parts();
                    println!("kv_key: {:?}", Tuple::from_bytes(kv_key)?);
                    println!("kv_value: {:?}", Tuple::from_bytes(kv_value)?);
                    println!();

                    let (mapped_range_begin_key, mapped_range_end_key) = mapped_range.into_parts();
                    println!(
                        "mapped_range_begin_key: {:?}",
                        Tuple::from_bytes(mapped_range_begin_key)?
                    );
                    // Not a tuple
                    println!("mapped_range_end_key: {:?}", mapped_range_end_key);
                    println!();

                    for mapped_kv in mapped_kvs {
                        let (mapped_kv_key, mapped_kv_value) = mapped_kv.into_parts();
                        println!("mapped_kv_key: {:?}", Tuple::from_bytes(mapped_kv_key)?);
                        println!("mapped_kv_value: {:?}", Tuple::from_bytes(mapped_kv_value)?);
                    }
                    println!("-----");
                }

                Ok(())
            })
            .await?;

        Result::<(), Box<dyn Error>>::Ok(())
    })?;

    drop(fdb_database);

    unsafe {
        fdb::stop_network();
    }

    Ok(())
}
