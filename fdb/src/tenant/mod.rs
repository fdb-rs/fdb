//! Provides [`FdbTenant`] type for working with FDB Tenants.
//!
//! Clients operating on a [`FdbTenant`] should, in most cases use the
//! [`run`] method. This implements a proper retry loop around the
//! work that needs to be done and, assure that [`commit`] has
//! returned successfully before returning.
//!
//! [`run`]: FdbTenant::run
//! [`commit`]: crate::transaction::Transaction::commit

mod fdb_tenant;
mod tenant_management;

pub(crate) mod tenant_inner;

pub use fdb_tenant::FdbTenant;
pub use tenant_management::TenantManagement;
