use crate::bytecode::plan::ExecutionPlan;
use crate::dsl::{compiler::Compiler, lexer, optimizer, parser};
use crate::error::FfiError;
use crate::executor::VM;
use crate::memory::{arena::Arena, intern::InternTable, slab::SlabPool};

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
        let pool = SlabPool::new();
        pool.prefill(1024, 512, 64);
        std::sync::Mutex::new(pool)
    });

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

fn read_slice<'a>(ptr: *const u8, len: usize) -> Option<&'a [u8]> {
    if ptr.is_null() {
        return None;
    }
    Some(unsafe { std::slice::from_raw_parts(ptr, len) })
}

fn read_str<'a>(ptr: *const u8, len: usize) -> Option<&'a str> {
    let slice = read_slice(ptr, len)?;
    std::str::from_utf8(slice).ok()
}

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
    let dsl_str = match read_str(dsl_ptr, dsl_len) {
        Some(s) => s,
        None => return FfiError::NullPointer.code(),
    };

    let rule_id = match read_str(rule_id_ptr, rule_id_len) {
        Some(s) => s,
        None => "default",
    };

    let tokens = match lexer::lex(dsl_str) {
        Ok(t) => t,
        Err(e) => {
            write_error(err_ptr, err_cap, err_len, &format!("flowrule_compile lex: {}", e));
            return FfiError::Lex.code();
        }
    };

    let pipeline = match parser::parse(&tokens) {
        Ok(p) => p,
        Err(e) => {
            write_error(err_ptr, err_cap, err_len, &format!("flowrule_compile parse: {}", e));
            return FfiError::Parse.code();
        }
    };

    let opt = optimizer::Optimizer::new();
    let optimized = opt.optimize(&pipeline);

    let compiler = Compiler::new(&[]);
    let plan = match compiler.compile(&optimized, rule_id) {
        Ok(p) => p,
        Err(e) => {
            write_error(err_ptr, err_cap, err_len, &format!("flowrule_compile: {}", e));
            return FfiError::Compile.code();
        }
    };

    let plan_bytes = match bincode::serialize(&plan) {
        Ok(b) => b,
        Err(e) => {
            write_error(err_ptr, err_cap, err_len, &format!("flowrule_compile serialize: {}", e));
            return FfiError::Serialize.code();
        }
    };

    if plan_bytes.len() > out_cap {
        write_error(
            err_ptr,
            err_cap,
            err_len,
            &format!(
                "flowrule_compile: output buffer too small ({} > {})",
                plan_bytes.len(),
                out_cap
            ),
        );
        return FfiError::BufferTooSmall.code();
    }

    unsafe {
        std::ptr::copy_nonoverlapping(plan_bytes.as_ptr(), out_ptr, plan_bytes.len());
        *out_len = plan_bytes.len();
    }

    0
}

#[no_mangle]
pub extern "C" fn flowrule_execute(
    plan_ptr: *const u8,
    plan_len: usize,
    body_ptr: *const u8,
    body_len: usize,
    caller_cb: extern "C" fn(u16, *const u8, usize, *mut u8, *mut usize) -> i32,
    out_ptr: *mut u8,
    out_cap: usize,
    out_len: *mut usize,
    err_ptr: *mut u8,
    err_cap: usize,
    err_len: *mut usize,
    msg_id_ptr: *const u8,
    msg_id_len: usize,
    corr_id_ptr: *const u8,
    corr_id_len: usize,
    trace_id_ptr: *const u8,
    trace_id_len: usize,
    partition: u32,
    offset: i64,
) -> i32 {
    let plan_slice = match read_slice(plan_ptr, plan_len) {
        Some(s) => s,
        None => return FfiError::NullPointer.code(),
    };

    let plan: ExecutionPlan = match bincode::deserialize(plan_slice) {
        Ok(p) => p,
        Err(e) => {
            write_error(err_ptr, err_cap, err_len, &format!("flowrule_execute deserialize: {}", e));
            return FfiError::Deserialize.code();
        }
    };

    let body = match read_slice(body_ptr, body_len) {
        Some(s) => s,
        None => return FfiError::NullPointer.code(),
    };

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

    if !msg_id_ptr.is_null() {
        if let Some(s) = read_str(msg_id_ptr, msg_id_len) {
            vm.ctx.message_id = s.to_string();
        }
    }
    if !corr_id_ptr.is_null() {
        if let Some(s) = read_str(corr_id_ptr, corr_id_len) {
            vm.ctx.correlation_id = s.to_string();
        }
    }
    if !trace_id_ptr.is_null() {
        if let Some(s) = read_str(trace_id_ptr, trace_id_len) {
            vm.ctx.trace_id = s.to_string();
        }
    }
    vm.ctx.partition = partition;
    vm.ctx.offset = offset;

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
                unsafe {
                    *err_len = 0;
                }
            }
            0
        }
        Err(e) => {
            write_error(err_ptr, err_cap, err_len, &format!("flowrule_execute: {}", e));
            FfiError::Exec.code()
        }
    }
}

#[no_mangle]
pub extern "C" fn flowrule_msg_alloc(_estimated_body_size: usize) -> *mut u8 {
    let _arena = SLAB_POOL.lock().unwrap().acquire(_estimated_body_size);
    let layout = std::alloc::Layout::new::<usize>();
    unsafe { std::alloc::alloc(layout) }
}

#[no_mangle]
pub extern "C" fn flowrule_msg_release(_ptr: *mut u8) {
    if !_ptr.is_null() {
        unsafe {
            let _ = std::boxed::Box::from_raw(_ptr);
        }
    }
}

#[no_mangle]
pub extern "C" fn flowrule_intern(s_ptr: *const u8, s_len: usize) -> u16 {
    let s = match read_str(s_ptr, s_len) {
        Some(s) => s,
        None => return 0,
    };
    INTERN_TABLE.intern(s)
}

#[no_mangle]
pub extern "C" fn flowrule_intern_lookup(id: u16, out_ptr: *mut u8, out_len: *mut usize) {
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
