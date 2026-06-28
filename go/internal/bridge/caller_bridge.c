#include <stdint.h>
#include <stddef.h>

typedef int (*caller_cb_t)(uint16_t, const unsigned char*, size_t, unsigned char*, size_t*);

extern int goServiceCaller(uint16_t, const unsigned char*, size_t, unsigned char*, size_t*);

static int callerBridge(uint16_t svc_id, const unsigned char* body, size_t body_len, unsigned char* resp, size_t* resp_len) {
    return goServiceCaller(svc_id, body, body_len, resp, resp_len);
}

caller_cb_t getCallerBridgePtr(void) {
    return callerBridge;
}
