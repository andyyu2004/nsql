/// An abstraction layer around [`rkyv::Archive`] and [`rkyv::ArchiveUnsized`].
/// This allows us to transparent use both sized and unsized types
/// Ideally, we would be able to create a default impl for `ArchiveUnsized`` and use a more
/// efficient specialization for `Archive` but not `ArchiveUnsized` types
/// but that doesn't seem possible without full specialiation?
pub trait Rkyv: Sized {
    /// The archived form of the type
    type Archived: rkyv::Deserialize<Self, rkyv::Infallible> + ?Sized + 'static;

    /// Serialize the type into a byte buffer
    fn serialize(&self) -> rkyv::AlignedVec;

    /// Get a reference to the archived form of the type
    /// You MUST NOT assume that the buffer is exactly the size of the archived type
    /// You may assume that the start of the buffer is the start of the bytes returned by [`Rkyv::serialize`]
    /// Store the size of the archived type in the buffer if necessary
    fn archived(buf: &[u8]) -> &Self::Archived;
}

impl<S> Rkyv for S
where
    S: rkyv::Serialize<nsql_rkyv::DefaultSerializer>,
    S::Archived: rkyv::Deserialize<Self, rkyv::Infallible> + 'static,
{
    type Archived = S::Archived;

    #[inline]
    fn serialize(&self) -> rkyv::AlignedVec {
        nsql_rkyv::to_bytes(self)
    }

    fn archived(buf: &[u8]) -> &Self::Archived {
        unsafe { nsql_rkyv::archived_value::<S>(buf, 0) }
    }
}
