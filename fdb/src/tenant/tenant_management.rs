use bytes::{BufMut, Bytes, BytesMut};

use std::sync::atomic::{AtomicBool, Ordering};

use crate::database::FdbDatabase;
use crate::error::{FdbError, FdbResult};
use crate::transaction::{FdbTransaction, ReadTransaction, Transaction, TransactionOption};
use crate::Tenant;

const TENANT_MAP_PREFIX: &[u8; 25] = b"\xFF\xFF/management/tenant_map/";

/// The FDB API includes function to manage the set of tenants in a
/// cluster.
#[derive(Debug)]
pub struct TenantManagement;

impl TenantManagement {
    /// Creates a new tenant in the cluster using a transaction
    /// created on the specified [`FdbDatabase`].
    pub async fn create_tenant_db(
        db: &FdbDatabase,
        tenant_name: impl Into<Tenant>,
    ) -> FdbResult<()> {
        let checked_existence = AtomicBool::new(false);
        let key = {
            let mut b = BytesMut::new();
            b.put(TENANT_MAP_PREFIX.as_ref());
            b.put(Into::<Bytes>::into(tenant_name.into()));
            Into::<Bytes>::into(b)
        };

        let checked_existence_ref = &checked_existence;
        let key_ref = &key;

        db.run(|tr| async move {
            tr.set_option(TransactionOption::SpecialKeySpaceEnableWrites)?;

            if checked_existence_ref.load(Ordering::SeqCst) {
                tr.set(key_ref.clone(), Bytes::new());
                Ok(())
            } else {
                let maybe_key = tr.get(key_ref.clone()).await?;

                checked_existence_ref.store(true, Ordering::SeqCst);

                match maybe_key {
                    None => {
                        tr.set(key_ref.clone(), Bytes::new());
                        Ok(())
                    }
                    Some(_) => {
                        // `tenant_already_exists` error
                        Err(FdbError::new(2132))
                    }
                }
            }
        })
        .await
    }

    /// Creates a new tenant in the cluster.
    pub fn create_tenant_tr(tr: &FdbTransaction, tenant_name: impl Into<Tenant>) -> FdbResult<()> {
        tr.set_option(TransactionOption::SpecialKeySpaceEnableWrites)?;

        tr.set(
            {
                let mut b = BytesMut::new();
                b.put(TENANT_MAP_PREFIX.as_ref());
                b.put(Into::<Bytes>::into(tenant_name.into()));
                Into::<Bytes>::into(b)
            },
            Bytes::new(),
        );

        Ok(())
    }

    /// Deletes a tenant from the cluster using a transaction created
    /// on the specified [`FdbDatabase`].
    pub async fn delete_tenant_db(
        db: &FdbDatabase,
        tenant_name: impl Into<Tenant>,
    ) -> FdbResult<()> {
        let checked_existence = AtomicBool::new(false);
        let key = {
            let mut b = BytesMut::new();
            b.put(TENANT_MAP_PREFIX.as_ref());
            b.put(Into::<Bytes>::into(tenant_name.into()));
            Into::<Bytes>::into(b)
        };

        let checked_existence_ref = &checked_existence;
        let key_ref = &key;

        db.run(|tr| async move {
            tr.set_option(TransactionOption::SpecialKeySpaceEnableWrites)?;

            if checked_existence_ref.load(Ordering::SeqCst) {
                tr.clear(key_ref.clone());
                Ok(())
            } else {
                let maybe_key = tr.get(key_ref.clone()).await?;

                checked_existence_ref.store(true, Ordering::SeqCst);

                match maybe_key {
                    None => {
                        // `tenant_not_found` error
                        Err(FdbError::new(2131))
                    }
                    Some(_) => {
                        tr.clear(key_ref.clone());
                        Ok(())
                    }
                }
            }
        })
        .await
    }

    /// Deletes a tenant from the cluster.
    pub fn delete_tenant_tr(tr: &FdbTransaction, tenant_name: impl Into<Tenant>) -> FdbResult<()> {
        tr.set_option(TransactionOption::SpecialKeySpaceEnableWrites)?;

        tr.clear({
            let mut b = BytesMut::new();
            b.put(TENANT_MAP_PREFIX.as_ref());
            b.put(Into::<Bytes>::into(tenant_name.into()));
            Into::<Bytes>::into(b)
        });

        Ok(())
    }
}
