#![feature(thread_id_value)]
#![cfg_attr(not(feature = "profile"), allow(dead_code))]

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
    pub physical_plan_explain: EventId,

    pub execute_event_id: EventId,
    pub execute_pipeline: EventId,
    pub execute_pipeline_create_source: EventId,
    pub execute_pipeline_source: EventId,
    pub execute_pipeline_execution_loop: EventId,
    pub execute_pipeline_operator: EventId,
    pub execute_pipeline_sink: EventId,
    pub execute_pipeline_finalize_sink: EventId,
    pub execute_expr_id: EventId,

    pub nlp_join_execute: EventId,
    pub hash_join_execute: EventId,
    pub cross_product_execute: EventId,

    thread_id: u32, // TODO, once we use multiple threads this won't be right
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
            physical_plan_explain: mk_id("explain"),
            execute_event_id: mk_id("execute"),
            execute_pipeline: mk_id("execute-pipeline"),
            execute_pipeline_create_source: mk_id("execute-pipeline-create-source"),
            execute_pipeline_source: mk_id("execute-pipeline-source"),
            execute_pipeline_execution_loop: mk_id("execute-pipeline-operator-loop"),
            execute_pipeline_operator: mk_id("execute-pipeline-operator"),
            execute_pipeline_sink: mk_id("execute-pipeline-sink"),
            execute_pipeline_finalize_sink: mk_id("execute-pipeline-finalize-sink"),
            execute_expr_id: mk_id("execute-expr"),
            nlp_join_execute: mk_id("nlp-join-execute"),
            hash_join_execute: mk_id("hash-join-execute"),
            cross_product_execute: mk_id("cross-product-execute"),
            generic_event_kind: profiler.alloc_string("generic"),
            // everything is currently single-threaded and always will be except for execution stuff
            thread_id: std::thread::current().id().as_u64().get() as u32,
            profiler,
        })
    }

    #[inline]
    #[cfg_attr(not(feature = "profile"), allow(unused_variables))]
    pub fn profile<R>(&self, event_id: EventId, f: impl FnOnce() -> R) -> R {
        #[cfg(feature = "profile")]
        let _guard = self.start(event_id);
        f()
    }

    #[cfg(feature = "profile")]
    #[inline]
    pub fn start(&self, event_id: EventId) -> impl Drop + '_ {
        self.profiler.start_recording_interval_event(
            self.generic_event_kind,
            event_id,
            self.thread_id,
        )
    }

    #[cfg(not(feature = "profile"))]
    #[inline]
    pub fn start(&self, _event_id: EventId) -> impl std::any::Any {}
}
