use bytes::Bytes;

use fdb::range::{Range, RangeOptions};
use fdb::transaction::{ReadTransaction, Transaction};
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

        // Clear the database.
        fdb_database
            .run(|tr| async move {
                tr.clear_range(Range::new(Bytes::new(), Bytes::from_static(b"\xFF")));

                Ok(())
            })
            .await?;

        // Set a few key values.
        fdb_database
            .run(|tr| async move {
                tr.set(Bytes::from("apple"), Bytes::from("foo"));
                tr.set(Bytes::from("cherry"), Bytes::from("baz"));
                tr.set(Bytes::from("banana"), Bytes::from("bar"));

                Ok(())
            })
            .await?;

        println!("non-snapshot range read");

        fdb_database
            .run(|tr| async move {
                let mut range_stream = tr.get_range(
                    KeySelector::first_greater_or_equal(Bytes::new()),
                    KeySelector::first_greater_or_equal(Bytes::from_static(b"\xFF")),
                    RangeOptions::default(),
                );

                while let Some(x) = range_stream.next().await {
                    let (key, value) = x?.into_parts();
                    println!(
                        "{} is {}",
                        String::from_utf8_lossy(&Bytes::from(key)[..]),
                        String::from_utf8_lossy(&Bytes::from(value)[..])
                    );
                }

                println!();

                let mut range_stream = Range::new(Bytes::new(), Bytes::from_static(b"\xFF"))
                    .into_stream(&tr, RangeOptions::default());

                while let Some(x) = range_stream.next().await {
                    let (key, value) = x?.into_parts();
                    println!(
                        "{} is {}",
                        String::from_utf8_lossy(&Bytes::from(key)[..]),
                        String::from_utf8_lossy(&Bytes::from(value)[..])
                    );
                }

                Ok(())
            })
            .await?;

        println!();
        println!("snapshot range read");

        fdb_database
            .read(|tr| async move {
                let mut range_stream = tr.get_range(
                    KeySelector::first_greater_or_equal(Bytes::new()),
                    KeySelector::first_greater_or_equal(Bytes::from_static(b"\xFF")),
                    RangeOptions::default(),
                );

                while let Some(x) = range_stream.next().await {
                    let (key, value) = x?.into_parts();
                    println!(
                        "{} is {}",
                        String::from_utf8_lossy(&Bytes::from(key)[..]),
                        String::from_utf8_lossy(&Bytes::from(value)[..])
                    );
                }

                println!();

                let mut range_stream = Range::new(Bytes::new(), Bytes::from_static(b"\xFF"))
                    .into_stream(&tr, RangeOptions::default());

                while let Some(x) = range_stream.next().await {
                    let (key, value) = x?.into_parts();
                    println!(
                        "{} is {}",
                        String::from_utf8_lossy(&Bytes::from(key)[..]),
                        String::from_utf8_lossy(&Bytes::from(value)[..])
                    );
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
