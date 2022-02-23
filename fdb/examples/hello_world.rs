use bytes::Bytes;

use fdb::transaction::{ReadTransaction, Transaction};

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

        fdb_database
            .run(|tr| async move {
                tr.set(Bytes::from("hello"), Bytes::from("world"));
                Ok(())
            })
            .await?;

        let ret = fdb_database
            .read(|rtr| async move { rtr.get(Bytes::from("hello")).await })
            .await?
            .expect("Key not found");

        println!(
            "snapshot get: hello, {}",
            String::from_utf8_lossy(&Bytes::from(ret)[..])
        );

        let ret = fdb_database
            .run(|tr| async move { tr.get(Bytes::from("hello")).await })
            .await?
            .expect("Key not found");

        println!(
            "non-snapshot get: hello, {}",
            String::from_utf8_lossy(&Bytes::from(ret)[..])
        );

        Result::<(), Box<dyn Error>>::Ok(())
    })?;

    drop(fdb_database);

    unsafe {
        fdb::stop_network();
    }

    Ok(())
}
