package bridge

/*
#cgo LDFLAGS: -L../../../rust/target/release -lflowrule_core -ldl
#include <stdlib.h>
#include <stdint.h>

typedef int (*caller_cb_t)(uint16_t, const unsigned char*, size_t, unsigned char*, size_t*);

int flowrule_compile(
    const unsigned char* dsl_ptr, size_t dsl_len,
    const unsigned char* rule_id_ptr, size_t rule_id_len,
    unsigned char* out_ptr, size_t out_cap, size_t* out_len,
    unsigned char* err_ptr, size_t err_cap, size_t* err_len
);

int flowrule_execute(
    const unsigned char* plan_ptr, size_t plan_len,
    const unsigned char* body_ptr, size_t body_len,
    caller_cb_t caller_cb,
    unsigned char* out_ptr, size_t out_cap, size_t* out_len,
    unsigned char* err_ptr, size_t err_cap, size_t* err_len
);

unsigned char* flowrule_msg_alloc(size_t size);
void flowrule_msg_release(unsigned char* ptr);
uint16_t flowrule_intern(const unsigned char* s_ptr, size_t s_len);
void flowrule_intern_lookup(uint16_t id, unsigned char* out_ptr, size_t* out_len);

caller_cb_t getCallerBridgePtr(void);
*/
import "C"

import (
	"fmt"
	"sync"
	"unsafe"
)

type ServiceCaller func(svcID uint16, body []byte) ([]byte, error)

var (
	callerMu     sync.Mutex
	activeCaller ServiceCaller
)

//export goServiceCaller
func goServiceCaller(svcID C.uint16_t, bodyPtr *C.uchar, bodyLen C.size_t, respPtr *C.uchar, respLen *C.size_t) C.int {
	body := C.GoBytes(unsafe.Pointer(bodyPtr), C.int(bodyLen))

	callerMu.Lock()
	cb := activeCaller
	callerMu.Unlock()

	if cb == nil {
		return -1
	}

	resp, err := cb(uint16(svcID), body)
	if err != nil {
		return -1
	}

	if len(resp) > 65536 {
		resp = resp[:65536]
	}
	copy((*[1 << 30]byte)(unsafe.Pointer(respPtr))[:len(resp):len(resp)], resp)
	*respLen = C.size_t(len(resp))
	return 0
}

func Compile(dsl string, ruleID string) ([]byte, error) {
	dslBytes := []byte(dsl)
	ridBytes := []byte(ruleID)

	outBuf := make([]byte, 256*1024)
	var outLen C.size_t
	errBuf := make([]byte, 4096)
	var errLen C.size_t

	rc := C.flowrule_compile(
		(*C.uchar)(unsafe.Pointer(&dslBytes[0])), C.size_t(len(dslBytes)),
		(*C.uchar)(unsafe.Pointer(&ridBytes[0])), C.size_t(len(ridBytes)),
		(*C.uchar)(unsafe.Pointer(&outBuf[0])), C.size_t(cap(outBuf)), &outLen,
		(*C.uchar)(unsafe.Pointer(&errBuf[0])), C.size_t(cap(errBuf)), &errLen,
	)
	if rc != 0 {
		return nil, fmt.Errorf("compile failed: %s", string(errBuf[:errLen]))
	}
	return outBuf[:outLen], nil
}

func Execute(plan []byte, body []byte, caller ServiceCaller) ([]byte, error) {
	callerMu.Lock()
	activeCaller = caller
	callerMu.Unlock()

	defer func() {
		callerMu.Lock()
		activeCaller = nil
		callerMu.Unlock()
	}()

	outBuf := make([]byte, 256*1024)
	var outLen C.size_t
	errBuf := make([]byte, 4096)
	var errLen C.size_t

	cb := C.getCallerBridgePtr()
	rc := C.flowrule_execute(
		(*C.uchar)(unsafe.Pointer(&plan[0])), C.size_t(len(plan)),
		(*C.uchar)(unsafe.Pointer(&body[0])), C.size_t(len(body)),
		cb,
		(*C.uchar)(unsafe.Pointer(&outBuf[0])), C.size_t(cap(outBuf)), &outLen,
		(*C.uchar)(unsafe.Pointer(&errBuf[0])), C.size_t(cap(errBuf)), &errLen,
	)
	if rc != 0 {
		return nil, fmt.Errorf("execute failed: %s", string(errBuf[:errLen]))
	}
	return outBuf[:outLen], nil
}

func MsgAlloc(size int) unsafe.Pointer {
	return unsafe.Pointer(C.flowrule_msg_alloc(C.size_t(size)))
}

func MsgRelease(ptr unsafe.Pointer) {
	C.flowrule_msg_release((*C.uchar)(ptr))
}

func Intern(s string) uint16 {
	b := []byte(s)
	return uint16(C.flowrule_intern((*C.uchar)(unsafe.Pointer(&b[0])), C.size_t(len(b))))
}

func InternLookup(id uint16) string {
	buf := make([]byte, 256)
	var outLen C.size_t
	C.flowrule_intern_lookup(C.uint16_t(id), (*C.uchar)(unsafe.Pointer(&buf[0])), &outLen)
	return string(buf[:outLen])
}
