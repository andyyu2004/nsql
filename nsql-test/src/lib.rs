pub use tempfile;

#[macro_export]
macro_rules! tmp {
    () => {
        $crate::tempfile::tempdir()?.path().join("test.db")
    };
}
