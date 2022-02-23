use bytes::Bytes;

use fdb::range::RangeOptions;
use fdb::subspace::Subspace;
use fdb::transaction::{MutationType, ReadTransaction, Transaction};
use fdb::tuple::{Tuple, Versionstamp};
use fdb::KeySelector;

use tokio::runtime::Runtime;
use tokio_stream::StreamExt;

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

        let tr_version = fdb_database
            .run(|tr| async move {
                let t = {
                    let mut tup = Tuple::new();
                    tup.add_string(String::from("prefix"));
                    tup.add_versionstamp(Versionstamp::incomplete(0));
                    tup
                };

                unsafe {
                    tr.mutate(
                        MutationType::SetVersionstampedKey,
                        t.pack_with_versionstamp(Bytes::new())?,
                        Bytes::new(),
                    );
                }

                Ok(unsafe { tr.get_versionstamp() })
            })
            .await?
            .get()
            .await?;

        let vs = fdb_database
            .run(|tr| async move {
                let subspace = Subspace::new(Bytes::new()).subspace(&{
                    let mut tup = Tuple::new();
                    tup.add_string("prefix".to_string());
                    tup
                });

                let subspace_range = subspace.range(&Tuple::new());

                let key = tr
                    .get_range(
                        KeySelector::first_greater_or_equal(subspace_range.begin().clone()),
                        KeySelector::first_greater_or_equal(subspace_range.end().clone()),
                        RangeOptions::default(),
                    )
                    .take(1)
                    .next()
                    .await
                    .unwrap()?
                    .get_key()
                    .clone();

                Ok(subspace
                    .unpack(&key.into())?
                    .get_versionstamp_ref(0)?
                    .clone())
            })
            .await?;

        assert_eq!(vs, Versionstamp::complete(tr_version, 0));

        Result::<(), Box<dyn Error>>::Ok(())
    })?;

    drop(fdb_database);

    unsafe {
        fdb::stop_network();
    }

    Ok(())
}
