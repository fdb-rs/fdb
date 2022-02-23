use bytes::Bytes;

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

        fdb_database
            .run(|tr| async move {
                tr.set(Bytes::from("k"), Bytes::from("0"));
                Ok(())
            })
            .await?;

        fdb_database
            .run(|tr| async move {
                // w0 = (grv, k, 0)
                let w0 = tr.watch(Bytes::from("k"));

                // The transaction that creates a watch needs to be
                // committed in order for the watch to be set
                // *globally*. A watch that isn’t committed can only
                // be triggered by modifications that happen in the
                // same transaction as the watch.
                tr.set(Bytes::from("k"), Bytes::from("1"));

                // We are triggering w0 locally.
                w0.await?;

                println!("w0 is ready");

                Ok(())
            })
            .await?;

        // `w1` is a global watch
        let w1 = fdb_database
            .run(|tr| async move {
                // w1 = (grv, k, 1)
                let w1 = tr.watch(Bytes::from("k"));
                Ok(w1)
            })
            .await?;

        // // If we do `w1.await` here now, we will block.
        // //
        // w1.await?;

        // The transaction below, will resolve the `w1` future as
        // we'll be updating `k`.
        fdb_database
            .run(|tr| async move {
                tr.set(Bytes::from("k"), Bytes::from("2"));
                Ok(())
            })
            .await?;

        // won't block.
        w1.await?;

        println!("w1 is ready");

        Result::<(), Box<dyn Error>>::Ok(())
    })?;

    drop(fdb_database);

    unsafe {
        fdb::stop_network();
    }

    Ok(())
}
