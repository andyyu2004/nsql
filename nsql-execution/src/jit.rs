use std::marker::PhantomData;
use std::mem;

use anyhow::Result;
use cranelift::prelude::*;
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module as _};
use nsql_catalog::expr::ExecutableExpr;
// use cranelift_jit::JITBuilder;
use nsql_catalog::Catalog;
use nsql_profile::Profiler;
use nsql_storage::expr::ExprOp;
use nsql_storage::tuple::{RawTupleLikeVTable, TupleLike};
use nsql_storage::value;
use nsql_storage_engine::StorageEngine;

use crate::TransactionContext;

/// We are allowed to assume pointers are 64-bits wide
#[cfg(not(target_pointer_width = "64"))]
compile_error!("nsql is only supported for 64-bit targets");

/// The JIT compiler compiling bytecode into native code.
#[derive(Default)]
pub(crate) struct Compiler {
    bcx: FunctionBuilderContext,
    stack: Vec<Value>,
    // ccx: codegen::Context,
    // builder: JITBuilder,
}

type JitFunctionPtr<'env, 'txn, S> = extern "C" fn(
    &'env S,
    &Profiler,                   // profiler to be passed around opaquely
    usize, // the transaction context to be passed around opaquely (&dyn is not ffi-safe, so we `to_raw_parts` the fat pointer)
    usize, // the dyn vtable
    *const (), // the `TupleLike` object pointer to evaluate against
    &'static RawTupleLikeVTable, // the ffi-safe vtable for the `TupleLike` object
    &mut value::Value, // out parameter for the result
) -> u32; // some exit code representing success or failure

pub struct JitFunction<'a, 'env, 'txn, S, M> {
    ptr: JitFunctionPtr<'env, 'txn, S>,
    _marker: PhantomData<&'a ExecutableExpr<'env, 'txn, S, M>>, // the function points to values owned by the expression
}

impl<'a, 'env, 'txn, S, M> JitFunction<'a, 'env, 'txn, S, M> {
    fn new(ptr: JitFunctionPtr<'env, 'txn, S>) -> Self {
        Self { ptr, _marker: PhantomData }
    }
}

impl Compiler {
    fn test_call_ffi_jit_fn<'env, 'txn, S: StorageEngine, M>(
        catalog: Catalog<'env, S>,
        profiler: &Profiler,
        tcx: &dyn TransactionContext<'env, 'txn, S, M>,
        tuple: &dyn TupleLike,
        f: JitFunction<'_, 'env, 'txn, S, M>,
    ) {
        let (tcx, tcx_vtable) =
            (tcx as *const dyn TransactionContext<'env, 'txn, S, M>).to_raw_parts();
        let (tuple, tuple_vtable) = tuple.to_raw_parts();

        let mut out = value::Value::Null;
        (f.ptr)(
            catalog.storage(),
            profiler,
            tcx as _,
            unsafe { mem::transmute(tcx_vtable) },
            tuple,
            tuple_vtable,
            &mut out,
        );
    }

    pub fn compile<'a, 'env, 'txn, S, M>(
        &mut self,
        module: &mut JITModule,
        expr: &'a ExecutableExpr<'env, 'txn, S, M>,
    ) -> Result<JitFunction<'a, 'env, 'txn, S, M>> {
        self.stack.clear();

        let mut ccx = module.make_context();
        let mut signature = module.make_signature();
        debug_assert!(signature.params.is_empty());
        // This needs to match `JitFunctionPtr`
        signature.params.extend(
            [
                types::I64, // *S
                types::I64, // *Profiler
                types::I64, // *dyn TransactionContext data pointer
                types::I64, // *dyn TransactionContext vtable pointer
                types::I64, // *const ()
                types::I64, // *const RawTupleLikeVTable
                types::I64, // *mut Value
            ]
            .into_iter()
            .map(AbiParam::new),
        );
        signature.returns.push(AbiParam::new(types::I32));
        ccx.func.signature = signature;

        let func = module.declare_function("tmp", Linkage::Local, &ccx.func.signature)?;

        let mut builder = FunctionBuilder::new(&mut ccx.func, &mut self.bcx);
        let block = builder.create_block();
        builder.append_block_params_for_function_params(block);
        builder.switch_to_block(block);
        builder.seal_block(block);
        builder.ensure_inserted_block();

        // let &[storage, profiler, tcx, tup, vtab, out] = builder.block_params(block) else {
        //     unreachable!()
        // };

        for op in expr.ops() {
            match op {
                ExprOp::Push(value) => {
                    // FIXME this relies on the lifetime of expr
                    self.stack.push(builder.ins().iconst(types::I64, value as *const _ as i64));
                }
                // FIXME this relies on the lifetime of the tuple
                ExprOp::Project { index: _ } => todo!(),
                ExprOp::MkArray { len: _ } => todo!(),
                ExprOp::Call { function: _ } => todo!(),
                ExprOp::IfNeJmp(_) => todo!(),
                ExprOp::IfNullJmp(_) => todo!(),
                ExprOp::Jmp(_) => todo!(),
                ExprOp::Dup => todo!(),
                ExprOp::Pop => todo!(),
                ExprOp::Ret => {
                    let _value = self.stack.pop().unwrap();
                    // todo write this to the out variable
                    let success = builder.ins().iconst(types::I32, 0);
                    builder.ins().return_(&[success]);
                }
            }
        }

        builder.seal_all_blocks();
        builder.finalize();

        module.define_function(func, &mut ccx)?;
        module.clear_context(&mut ccx);
        module.finalize_definitions()?;
        Ok(JitFunction::new(unsafe { mem::transmute(module.get_finalized_function(func)) }))
    }
}

pub(crate) struct Module {
    module: JITModule,
}

impl Module {
    pub(crate) fn new() -> Result<Self> {
        let builder = JITBuilder::new(cranelift_module::default_libcall_names())?;
        let module = JITModule::new(builder);
        Ok(Self { module })
    }

    pub(crate) fn compile<'a, 'env, 'txn, S, M>(
        &mut self,
        expr: &'a ExecutableExpr<'env, 'txn, S, M>,
    ) -> Result<JitFunction<'a, 'env, 'txn, S, M>> {
        let mut compiler = Compiler::default();
        compiler.compile(&mut self.module, expr)
    }
}

#[cfg(test)]
mod tests;
