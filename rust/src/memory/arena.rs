use bumpalo::Bump;

pub struct Arena {
    bump: Bump,
}

unsafe impl Send for Arena {}

impl Arena {
    pub fn new() -> Self {
        Arena { bump: Bump::new() }
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

    pub fn allocated_bytes(&self) -> usize {
        self.bump.allocated_bytes()
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}
