use crate::PageIndex;

pub(crate) struct MetaPageReader<'a, P> {
    pager: &'a P,
    page_idx: PageIndex,
}

impl<'a, P> MetaPageReader<'a, P> {
    pub(crate) fn new(pager: &'a P, page_idx: PageIndex) -> Self {
        assert!(page_idx != PageIndex::INVALID);
        Self { pager, page_idx }
    }
}
