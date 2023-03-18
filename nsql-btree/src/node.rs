use rkyv::Archive;

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy)]
    pub struct Flags: u8 {
        const IS_ROOT = 1 << 0;
        const IS_LEAF = 1 << 1;
    }
}

impl Archive for Flags {
    type Archived = Flags;
    type Resolver = ();

    unsafe fn resolve(&self, _: usize, (): Self::Resolver, out: *mut Self::Archived) {
        out.write(*self);
    }
}

impl<S: rkyv::ser::Serializer + ?Sized> rkyv::Serialize<S> for Flags {
    fn serialize(
        &self,
        _serializer: &mut S,
    ) -> Result<Self::Resolver, <S as rkyv::Fallible>::Error> {
        Ok(())
    }
}
