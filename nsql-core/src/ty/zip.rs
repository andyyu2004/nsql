use super::LogicalType;

pub type ZipResult<T> = Result<T, ZipError>;

pub struct ZipError;

pub trait Zipper {
    fn zip_tys(&mut self, a: &LogicalType, b: &LogicalType) -> ZipResult<()>;
}

pub trait Zip {
    fn zip_with<Z: Zipper>(zipper: &mut Z, a: &Self, b: &Self) -> ZipResult<()>;
}

impl Zip for LogicalType {
    #[inline]
    fn zip_with<Z: Zipper>(zipper: &mut Z, a: &Self, b: &Self) -> ZipResult<()> {
        zipper.zip_tys(a, b)
    }
}

impl<T: Zip, const N: usize> Zip for [T; N] {
    fn zip_with<Z: Zipper>(zipper: &mut Z, a: &Self, b: &Self) -> ZipResult<()> {
        Zip::zip_with(zipper, &a[..], &b[..])
    }
}

impl<T: Zip> Zip for [T] {
    fn zip_with<Z: Zipper>(zipper: &mut Z, a: &Self, b: &Self) -> ZipResult<()> {
        if a.len() != b.len() {
            return Err(ZipError);
        }

        for (a_elem, b_elem) in a.iter().zip(b) {
            Zip::zip_with(zipper, a_elem, b_elem)?;
        }

        Ok(())
    }
}
