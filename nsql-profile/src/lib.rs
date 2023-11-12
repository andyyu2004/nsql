#![feature(thread_id_value)]

use std::error::Error;
use std::path::Path;

use measureme::{EventId, StringId};

pub struct Profiler {
    profiler: measureme::Profiler,
    generic_event_kind: StringId,

    pub debug_event_id: EventId,

    pub bind_event_id: EventId,

    pub optimize_event_id: EventId,
    pub opt_transform_event_id: EventId,
    pub opt_egraph_event_id: EventId,
    pub opt_build_egraph_event_id: EventId,

    pub physical_plan_event_id: EventId,
    pub physical_plan_compile_event_id: EventId,
    pub physical_plan_compile_function_lookup_event_id: EventId,
    pub physical_plan_explain_event_id: EventId,

    pub execute_event_id: EventId,

    thread_id: u32,
}

impl Profiler {
    pub fn new(path: &Path) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let profiler = measureme::Profiler::new(path)?;
        let mk_id = |s: &str| EventId::from_label(profiler.alloc_string(s));

        Ok(Self {
            debug_event_id: mk_id("debug"),
            bind_event_id: mk_id("bind"),
            optimize_event_id: mk_id("optimize"),
            opt_transform_event_id: mk_id("opt-transform"),
            opt_egraph_event_id: mk_id("opt-egraph-build"),
            opt_build_egraph_event_id: mk_id("opt-egraph-optimize"),
            physical_plan_event_id: mk_id("physical-plan"),
            physical_plan_compile_event_id: mk_id("compile"),
            physical_plan_compile_function_lookup_event_id: mk_id("compile-function-lookup"),
            physical_plan_explain_event_id: mk_id("explain"),
            execute_event_id: mk_id("execute"),
            generic_event_kind: profiler.alloc_string("generic"),
            // everything is currently single-threaded and always will be except for execution stuff
            thread_id: std::thread::current().id().as_u64().get() as u32,
            profiler,
        })
    }

    pub fn profile<R>(&self, event_id: EventId, f: impl FnOnce() -> R) -> R {
        let _guard = self.profiler.start_recording_interval_event(
            self.generic_event_kind,
            event_id,
            self.thread_id,
        );
        f()
    }
}
