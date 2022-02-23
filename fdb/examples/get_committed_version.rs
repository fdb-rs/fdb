use bytes::Bytes;

use fdb::error::FdbResult;
use fdb::transaction::Transaction;

use tokio::runtime::Runtime;

use std::env;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let fdb_cluster_file = env::var("FDB_CLUSTER_FILE").expect("FDB_CLUSTER_FILE not defined!");

    unsafe {
        fdb::select_api_version(fdb::FDB_API_VERSION as i32);
        fdb::start_network();
    }

    let fdb_database = fdb::open_database(fdb_cluster_file)?;

    let rt = Runtime::new()?;

    let cloned_fdb_database = fdb_database.clone();

    rt.block_on(async {
        let fdb_database = cloned_fdb_database;

        let committed_version = fdb_database
            .run(|tr| async move {
                tr.set(Bytes::from("hello"), Bytes::from("world"));
                Ok(unsafe { tr.get_committed_version() })
            })
            .await?;

        println!(
            "get_commited_version: {:?}",
            Into::<FdbResult<i64>>::into(committed_version)?
        );

        Result::<(), Box<dyn Error>>::Ok(())
    })?;

    drop(fdb_database);

    unsafe {
        fdb::stop_network();
    }

    Ok(())
}
