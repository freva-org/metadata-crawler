//! POSIX filesystem helper functions for the metadata crawler.
//!
//! This module provides high-performance asynchronous wrappers around
//! common filesystem operations such as directory checks, file checks,
//! directory iteration, and recursive globbing. The functions are exposed
//! to Python via PyO3 and serve as an optimized backend for the
//! `metadata_crawler` Python package.
//!
//! Unlike Python's `pathlib` and `os` modules, all filesystem access in this
//! module is performed inside a Tokio thread pool. This ensures that callers
//! using Python's `asyncio` event loop or `anyio` remain fully non-blocking,
//! even for deep recursive directory scans.
//!
//! The provided helpers are intended to accelerate the metadata crawler's
//! POSIX storage backend, especially when crawling large directory trees or
//! scanning high-latency filesystems.
//!
//! Functions
//! ---------
//! - [`is_dir`] — Check whether a path is a directory.
//! - [`is_file`] — Check whether a path is a file.
//! - [`iterdir`] — List immediate directory children.
//! - [`rglob`] — Recursively search for files matching a glob pattern.
//!
//! All functions return Python awaitables and integrate cleanly with
//! any asyncio or anyio-based workflow.

#![allow(unsafe_op_in_unsafe_fn)]

use globset::{GlobBuilder, GlobMatcher};
use pyo3::prelude::*;
use pyo3::{wrap_pyfunction};
use pyo3_asyncio::tokio::future_into_py;
use std::path::PathBuf;
use tokio::fs;
use tokio::task;
use walkdir::WalkDir;

/// Asynchronously check whether a given filesystem path is a directory.
///
/// Parameters
/// ----------
/// path : str
///     The POSIX filesystem path to test.
///
/// Returns
/// -------
/// bool
///     ``True`` if the path exists and is a directory; ``False`` otherwise.
///
/// Notes
/// -----
/// This function executes the directory check inside a Tokio threadpool
/// so it does not block the Python event loop.
#[pyfunction]
#[pyo3(signature = (path))]
pub fn is_dir(py: Python<'_>, path: String) -> PyResult<&PyAny> {
    future_into_py(py, async move {
        let meta = fs::metadata(&path).await;
        Ok(meta.map(|m| m.is_dir()).unwrap_or(false))
    })
}

/// Asynchronously check whether a given filesystem path is a regular file.
///
/// Parameters
/// ----------
/// path : str
///     The POSIX filesystem path to test.
///
/// Returns
/// -------
/// bool
///     ``True`` if the path exists and is a file; ``False`` otherwise.
///
/// Notes
/// -----
/// This function performs an async-friendly file check by delegating
/// filesystem access to a Tokio worker thread. If the file does not
/// exist or cannot be accessed, the function returns ``False``.
#[pyfunction]
#[pyo3(signature = (path))]
pub fn is_file(py: Python<'_>, path: String) -> PyResult<&PyAny> {
    future_into_py(py, async move {
        let meta = fs::metadata(&path).await;
        Ok(meta.map(|m| m.is_file()).unwrap_or(false))
    })
}


/// Asynchronously list the immediate entries inside a directory.
///
/// Parameters
/// ----------
/// path : str
///     The POSIX directory to iterate.
///
/// Returns
/// -------
/// list[str]
///     A list of full paths to all immediate children (files or directories).
///
/// Behavior
/// --------
/// * If the path is a directory, returns its first-level children.
/// * If the path is a file, the result contains only the path itself.
/// * If the path does not exist, an empty list is returned.
///
/// Notes
/// -----
/// This function uses Tokio filesystem APIs and returns the complete
/// result list as a Python ``list``. It does not block the Python event
/// loop.
#[pyfunction]
#[pyo3(signature = (path))]
pub fn iterdir(py: Python<'_>, path: String) -> PyResult<&PyAny> {
    future_into_py(py, async move {
        let pathbuf = PathBuf::from(&path);

        let meta = match fs::metadata(&path).await {
            Ok(m) => m,
            Err(_) => return Ok(Vec::<String>::new()),
        };

        if !meta.is_dir() {
            return Ok(vec![path]);
        }

        let entries = task::spawn_blocking(move || {
            let mut out = Vec::new();
            if let Ok(read_dir) = std::fs::read_dir(&pathbuf) {
                for entry in read_dir.flatten() {
                    out.push(entry.path().to_string_lossy().into_owned());
                }
            }
            out
        })
        .await
        .unwrap();

        Ok(entries)
    })
}

/// Recursively search a directory tree for files matching a glob pattern.
///
/// Parameters
/// ----------
/// path : str
///     The root path from which to start the recursive search.
/// glob_pattern : str, optional
///     A glob pattern matching file names (default: ``"*"``).
/// suffixes : list[str] or None, optional
///     If provided, only matching files with these suffixes are returned.
///     If ``None``, no suffix filtering is applied.
///
/// Returns
/// -------
/// list[str]
///     A list of POSIX filesystem paths matching the pattern and suffix filters.
///
/// Behavior
/// --------
/// * If ``path`` refers to a file, it is returned immediately.
/// * If ``path`` refers to a directory, the function walks the entire subtree.
/// * Hidden files are included.
/// * Broken symlinks are ignored.
///
/// Notes
/// -----
/// The filesystem traversal is performed using the ``walkdir`` crate inside
/// a Tokio worker thread, making the call async-friendly without blocking
/// Python's event loop.
///
/// This function is intended as a high-performance backend replacement for
/// Python's ``pathlib.Path.rglob()`` in the metadata crawler.
#[pyfunction]
#[pyo3(signature = (path, glob_pattern=None, suffixes=None))]
pub fn rglob(
    py: Python<'_>,
    path: String,
    glob_pattern: Option<String>,
    suffixes: Option<Vec<String>>,
) -> PyResult<&PyAny> {
    future_into_py(py, async move {
        let root = PathBuf::from(&path);
        let pattern = glob_pattern.unwrap_or_else(|| "*".to_string());
        let suffixes: Vec<String> = suffixes
            .unwrap_or_default()
            .into_iter()
            .map(|s| s.to_lowercase())
            .collect();

        let results = task::spawn_blocking(move || {
            let p = root.as_path();
            let mut out: Vec<String> = Vec::new();

            let is_file = p.is_file();
            let is_zarr = p
                .extension()
                .and_then(|e| e.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("zarr"))
                .unwrap_or(false);

            if is_file || is_zarr {
                out.push(p.to_string_lossy().into_owned());
                return out;
            }

            let matcher: Option<GlobMatcher> = if pattern == "*" {
                None
            } else {
                GlobBuilder::new(&pattern)
                    .literal_separator(true)
                    .build()
                    .ok()
                    .map(|g| g.compile_matcher())
            };

            for entry in WalkDir::new(p).into_iter().flatten() {
                let entry_path = entry.path();

                if !entry_path.is_file() {
                    continue;
                }

                if let Some(m) = &matcher {
                    let rel = entry_path.strip_prefix(p).unwrap_or(entry_path);
                    if !m.is_match(rel) {
                        continue;
                    }
                }

                if let Some(ext) = entry_path.extension().and_then(|e| e.to_str()) {
                    let with_dot = format!(".{}", ext.to_lowercase());
                    if !suffixes.is_empty() && !suffixes.contains(&with_dot) {
                        continue;
                    }
                } else {
                    continue;
                }

                out.push(entry_path.to_string_lossy().into_owned());
            }

            out
        })
        .await
        .unwrap();

        Ok(results)
    })
}

pub fn register(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(is_dir, m)?)?;
    m.add_function(wrap_pyfunction!(is_file, m)?)?;
    m.add_function(wrap_pyfunction!(iterdir, m)?)?;
    m.add_function(wrap_pyfunction!(rglob, m)?)?;
    Ok(())
}
