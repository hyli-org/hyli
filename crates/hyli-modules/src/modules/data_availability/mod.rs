pub mod blocks_memory;

#[cfg(feature = "fjall")]
pub mod blocks_fjall;
#[cfg(not(feature = "fjall"))]
pub use blocks_memory as blocks_fjall;

pub use blocks_fjall::Blocks;
