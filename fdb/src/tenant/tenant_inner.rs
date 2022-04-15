// We name this module `tenant_inner` in order to avoid
// `module_inception` lint. We need to do this because we want to
// expose `Tenant` type as `crate::Tenant`.

use bytes::Bytes;

/// [`Tenant`] is a named key-space within a database.
///
/// [`Tenant`] can be converted from and into [`Bytes`].
///
/// **Note:** Tenant should not begin with `\xFF`. We do not enforce
/// this check when creating a value of type [`Tenant`]. If you create
/// a value of [`Tenant`] that starts with `\xFF`, you can expect
/// tentant operations to fail with an [`FdbError`] code of 2134
/// (`invalid_tenant_name`).
///
/// [`FdbError`]: crate::error::FdbError
#[derive(Clone, Debug, PartialEq)]
pub struct Tenant(Bytes);

impl From<Bytes> for Tenant {
    fn from(b: Bytes) -> Tenant {
        Tenant(b)
    }
}

impl From<Tenant> for Bytes {
    fn from(t: Tenant) -> Bytes {
        t.0
    }
}
