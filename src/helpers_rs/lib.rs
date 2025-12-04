//! Internal Rust extension for the `metadata_crawler` package.
//!
//! This crate exposes high-performance helper modules written in Rust,
//! designed to accelerate the metadata crawler's storage backends.
//!
//! The compiled library is packaged as `metadata_crawler._helper` and
//! integrates seamlessly with Python via PyO3, providing async-friendly
//! implementations of performance-critical operations.
//!
//! Architecture
//! ------------
//! - Each backend (e.g. POSIX, S3, Swift, etc.) is implemented in a separate
//!   Rust module.
//! - Every backend module exports a `register()` function that attaches its
//!   functions to a Python submodule.
//! - The top-level `_helper` module aggregates these backend submodules.
//!
//! For example, the POSIX backend is exported as:
//!
//! ```python
//! from metadata_crawler import _helper
//! _helper.posix.is_dir("/tmp")
//! ```
//!
//! This structure makes it easy to add more optimized backends without
//! modifying the Python-level API.
//!
//! The crate uses:
//! - **Tokio** for async execution without blocking Python's event loop.
//! - **PyO3** for Python bindings.
//! - **walkdir** and **globset** for efficient filesystem traversal and pattern
//!   matching.
//!
//! Additional backends or helper modules can be registered by adding them to
//! the `_helper` initializer.

mod posix_backend;
pub mod utils;

use pyo3::prelude::*;

#[pymodule]
fn _helper(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    // Create submodule: metadata_crawler._helper.posix
    let posix_mod = PyModule::new(_py, "posix")?;
    posix_backend::register(_py, &posix_mod)?;
    m.add_submodule(posix_mod)?;

    Ok(())
}
