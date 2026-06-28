use bumpalo::Bump;
use super::slab::SlabPool;

pub struct Arena {
    bump: Bump,
    slab_pool: *mut SlabPool,
}

unsafe impl Send for Arena {}

impl Arena {
    pub fn new() -> Self {
        Arena {
            bump: Bump::new(),
            slab_pool: std::ptr::null_mut(),
        }
    }

    pub fn with_slab_pool(pool: &SlabPool) -> Self {
        Arena {
            bump: Bump::new(),
            slab_pool: pool as *const SlabPool as *mut SlabPool,
        }
    }

    pub fn alloc(&self, n: usize) -> &mut [u8] {
        self.bump.alloc_slice_fill_default(n)
    }

    pub fn alloc_copy(&self, src: &[u8]) -> &mut [u8] {
        let buf = self.alloc(src.len());
        buf.copy_from_slice(src);
        buf
    }

    pub fn alloc_str(&self, s: &str) -> &mut [u8] {
        self.alloc_copy(s.as_bytes())
    }

    pub fn reset(&mut self) {
        self.bump.reset();
    }

    pub fn release(mut self) {
        self.bump.reset();
        if !self.slab_pool.is_null() {
            unsafe {
                (*self.slab_pool).put(self);
            }
        }
    }

    pub fn allocated_bytes(&self) -> usize {
        self.bump.allocated_bytes()
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}
