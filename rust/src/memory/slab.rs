use crossbeam_queue::SegQueue;
use super::arena::Arena;

const SMALL_SIZE: usize = 2048;
const MEDIUM_SIZE: usize = 8192;
const LARGE_SIZE: usize = 65536;

pub struct SlabPool {
    pub small: SegQueue<Arena>,
    pub medium: SegQueue<Arena>,
    pub large: SegQueue<Arena>,
}

impl SlabPool {
    pub fn new() -> Self {
        SlabPool {
            small: SegQueue::new(),
            medium: SegQueue::new(),
            large: SegQueue::new(),
        }
    }

    pub fn prefill(&self, small: usize, medium: usize, large: usize) {
        for _ in 0..small {
            self.small.push(Arena::with_slab_pool(self));
        }
        for _ in 0..medium {
            self.medium.push(Arena::with_slab_pool(self));
        }
        for _ in 0..large {
            self.large.push(Arena::with_slab_pool(self));
        }
    }

    pub fn acquire(&self, estimated_size: usize) -> Arena {
        let arena = if estimated_size <= SMALL_SIZE {
            self.small.pop()
        } else if estimated_size <= MEDIUM_SIZE {
            self.medium.pop()
        } else {
            self.large.pop()
        };

        match arena {
            Some(mut a) => {
                a.reset();
                a
            }
            None => Arena::new(),
        }
    }

    pub fn put(&self, mut arena: Arena) {
        arena.reset();
        let cap = match arena.allocated_bytes() {
            n if n <= SMALL_SIZE => &self.small,
            n if n <= MEDIUM_SIZE => &self.medium,
            _ => &self.large,
        };
        cap.push(arena);
    }
}

impl Default for SlabPool {
    fn default() -> Self {
        Self::new()
    }
}
