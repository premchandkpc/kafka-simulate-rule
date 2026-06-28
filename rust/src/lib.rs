pub mod bytecode;
pub mod dsl;
pub mod executor;
pub mod memory;

use std::ffi::CStr;
use std::os::raw::c_char;

use bytecode::instruction::Instruction;
use bytecode::plan::ExecutionPlan;
use dsl::{lexer, parser, optimizer, compiler::Compiler};
use executor::VM;
use memory::{arena::Arena, slab::SlabPool, intern::InternTable};

static INTERN_TABLE: once_cell::sync::Lazy<InternTable> = once_cell::sync::Lazy::new(|| {
    let table = InternTable::new();
    table.prefill(&[
        "content-type",
        "content-length",
        "x-correlation-id",
        "x-trace-id",
        "x-flowrule-chunk-id",
        "x-flowrule-chunk-index",
        "x-flowrule-chunk-total",
    ]);
    table
});

static SLAB_POOL: once_cell::sync::Lazy<std::sync::Mutex<SlabPool>> =
    once_cell::sync::Lazy::new(|| {
        let mut pool = SlabPool::new();
        pool.prefill(1024, 512, 64);
        std::sync::Mutex::new(pool)
    });

/// Compile a DSL string into an ExecutionPlan byte array.
/// Returns 0 on success, negative on error.
#[no_mangle]
pub extern "C" fn flowrule_compile(
    dsl_ptr: *const u8,
    dsl_len: usize,
    rule_id_ptr: *const u8,
    rule_id_len: usize,
    out_ptr: *mut u8,
    out_cap: usize,
    out_len: *mut usize,
    err_ptr: *mut u8,
    err_cap: usize,
    err_len: *mut usize,
) -> i32 {
    if dsl_ptr.is_null() || out_ptr.is_null() || out_len.is_null() {
        return -1;
    }

    let dsl_slice = unsafe { std::slice::from_raw_parts(dsl_ptr, dsl_len) };
    let dsl_str = match std::str::from_utf8(dsl_slice) {
        Ok(s) => s,
        Err(e) => {
            let msg = format!("flowrule_compile: invalid utf8 dsl: {}", e);
            write_error(err_ptr, err_cap, err_len, &msg);
            return -2;
        }
    };

    let rule_id = if rule_id_ptr.is_null() || rule_id_len == 0 {
        "default"
    } else {
        let rid_slice = unsafe { std::slice::from_raw_parts(rule_id_ptr, rule_id_len) };
        match std::str::from_utf8(rid_slice) {
            Ok(s) => s,
            Err(_) => "default",
        }
    };

    let tokens = match lexer::lex(dsl_str) {
        Ok(t) => t,
        Err(e) => {
            let msg = format!("flowrule_compile lex: {}", e);
            write_error(err_ptr, err_cap, err_len, &msg);
            return -3;
        }
    };

    let pipeline = match parser::parse(&tokens) {
        Ok(p) => p,
        Err(e) => {
            let msg = format!("flowrule_compile parse: {}", e);
            write_error(err_ptr, err_cap, err_len, &msg);
            return -4;
        }
    };

    let opt = optimizer::Optimizer::new();
    let optimized = opt.optimize(&pipeline);

    let compiler = Compiler::new(&[]);
    let plan = match compiler.compile(&optimized, rule_id) {
        Ok(p) => p,
        Err(e) => {
            let msg = format!("flowrule_compile: {}", e);
            write_error(err_ptr, err_cap, err_len, &msg);
            return -5;
        }
    };

    let plan_bytes = match bincode::serialize(&plan) {
        Ok(b) => b,
        Err(e) => {
            let msg = format!("flowrule_compile serialize: {}", e);
            write_error(err_ptr, err_cap, err_len, &msg);
            return -6;
        }
    };

    if plan_bytes.len() > out_cap {
        let msg = format!(
            "flowrule_compile: output buffer too small ({} > {})",
            plan_bytes.len(),
            out_cap
        );
        write_error(err_ptr, err_cap, err_len, &msg);
        return -7;
    }

    unsafe {
        std::ptr::copy_nonoverlapping(plan_bytes.as_ptr(), out_ptr, plan_bytes.len());
        *out_len = plan_bytes.len();
    }

    0
}

/// Execute a compiled plan against a message.
#[no_mangle]
pub extern "C" fn flowrule_execute(
    plan_ptr: *const u8,
    plan_len: usize,
    body_ptr: *const u8,
    body_len: usize,
    caller_cb: extern "C" fn(
        u16,
        *const u8,
        usize,
        *mut u8,
        *mut usize,
    ) -> i32,
    out_ptr: *mut u8,
    out_cap: usize,
    out_len: *mut usize,
    err_ptr: *mut u8,
    err_cap: usize,
    err_len: *mut usize,
) -> i32 {
    if plan_ptr.is_null() || body_ptr.is_null() {
        return -1;
    }

    let plan_slice = unsafe { std::slice::from_raw_parts(plan_ptr, plan_len) };
    let plan: ExecutionPlan = match bincode::deserialize(plan_slice) {
        Ok(p) => p,
        Err(e) => {
            let msg = format!("flowrule_execute deserialize plan: {}", e);
            write_error(err_ptr, err_cap, err_len, &msg);
            return -2;
        }
    };

    let body = unsafe { std::slice::from_raw_parts(body_ptr, body_len) };

    let arena = Arena::new();
    let caller_wrapper = |svc_id: u16, b: &[u8], _timeout: u64| -> Result<Vec<u8>, String> {
        let mut resp_buf = vec![0u8; 65536];
        let mut resp_len: usize = 0;

        let rc = caller_cb(
            svc_id,
            b.as_ptr(),
            b.len(),
            resp_buf.as_mut_ptr(),
            &mut resp_len as *mut usize,
        );

        if rc != 0 {
            Err(format!("caller returned {}", rc))
        } else {
            resp_buf.truncate(resp_len);
            Ok(resp_buf)
        }
    };

    let mut vm = VM::new(&plan, body, arena, &caller_wrapper);
    match vm.run() {
        Ok(()) => {
            let result = &vm.last_response;
            if result.len() <= out_cap {
                unsafe {
                    std::ptr::copy_nonoverlapping(result.as_ptr(), out_ptr, result.len());
                    *out_len = result.len();
                }
            }
            if !err_ptr.is_null() && err_cap > 0 {
                unsafe { *err_len = 0; }
            }
            0
        }
        Err(e) => {
            let msg = format!("flowrule_execute: {}", e);
            write_error(err_ptr, err_cap, err_len, &msg);
            -3
        }
    }
}

/// Allocate a Message from the slab pool.
#[no_mangle]
pub extern "C" fn flowrule_msg_alloc(_estimated_body_size: usize) -> *mut u8 {
    let arena = SLAB_POOL.lock().unwrap().acquire(_estimated_body_size);
    let layout = std::alloc::Layout::new::<usize>();
    let ptr = unsafe { std::alloc::alloc(layout) };
    ptr
}

/// Release a Message back to the pool.
#[no_mangle]
pub extern "C" fn flowrule_msg_release(_ptr: *mut u8) {
    if !_ptr.is_null() {
        unsafe {
            let _ = std::boxed::Box::from_raw(_ptr);
        }
    }
}

/// Intern a string, returning a u16 ID.
#[no_mangle]
pub extern "C" fn flowrule_intern(s_ptr: *const u8, s_len: usize) -> u16 {
    if s_ptr.is_null() || s_len == 0 {
        return 0;
    }
    let s_slice = unsafe { std::slice::from_raw_parts(s_ptr, s_len) };
    let s = match std::str::from_utf8(s_slice) {
        Ok(s) => s,
        Err(_) => return 0,
    };
    INTERN_TABLE.intern(s)
}

/// Lookup an interned string by ID.
#[no_mangle]
pub extern "C" fn flowrule_intern_lookup(
    id: u16,
    out_ptr: *mut u8,
    out_len: *mut usize,
) {
    if out_ptr.is_null() || out_len.is_null() {
        return;
    }
    if let Some(s) = INTERN_TABLE.lookup(id) {
        let bytes = s.as_bytes();
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, bytes.len());
            *out_len = bytes.len();
        }
    }
}

fn write_error(ptr: *mut u8, cap: usize, len: *mut usize, msg: &str) {
    if ptr.is_null() || cap == 0 || len.is_null() {
        return;
    }
    let bytes = msg.as_bytes();
    let n = bytes.len().min(cap);
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, n);
        *len = n;
    }
}
