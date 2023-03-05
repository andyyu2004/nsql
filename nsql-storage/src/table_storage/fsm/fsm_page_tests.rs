use nsql_pager::PageOffset;
use test_strategy::proptest;

use super::fsm_page::{FsmPage, MAX_OFFSET};
use crate::table_storage::HeapTuple;

#[proptest]
fn test_fsm_page_default_has_no_free_space(#[strategy(1..HeapTuple::MAX_SIZE)] required: u16) {
    let fsm_page = FsmPage::default();
    let offset = fsm_page.find(required);
    assert!(offset.is_none());
}

#[proptest]
fn test_fsm_page_cant_find_unavailable_free_space(
    #[strategy(1..HeapTuple::MAX_SIZE)] required: u16,
    #[strategy(0..MAX_OFFSET)] offset: u16,
) {
    let mut fsm_page = FsmPage::default();
    fsm_page.update(PageOffset::new(offset as u32), required);
    let offset = fsm_page.find(required + 1);
    assert!(offset.is_none());
}

#[proptest]
fn test_fsm_page_can_always_find_space_on_empty_page(
    #[strategy(1..HeapTuple::MAX_SIZE)] required: u16,
    #[strategy(0..MAX_OFFSET)] offset: u16,
) {
    let mut fsm_page = FsmPage::default();
    let expected = PageOffset::new(offset as u32);
    fsm_page.update(expected, HeapTuple::MAX_SIZE);
    let offset = fsm_page.find(required);
    assert_eq!(offset, Some(expected));
}

#[test]
fn test_fsm_page_edge_cases() {
    for offset in [0, MAX_OFFSET as u32] {
        let mut fsm_page = FsmPage::default();
        let offset = PageOffset::new(offset);
        fsm_page.update(offset, HeapTuple::MAX_SIZE);
        assert_eq!(fsm_page.find(1), Some(offset));

        assert_eq!(fsm_page.find(HeapTuple::MAX_SIZE), Some(offset));
    }
}

#[test]
fn test_fsm_page_bucketing() {
    let mut fsm_page = FsmPage::default();
    let offset = PageOffset::new(20);
    fsm_page.update(offset, 1);
    assert!(fsm_page.find(2).is_none());
}
