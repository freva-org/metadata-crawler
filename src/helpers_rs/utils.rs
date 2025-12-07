///Utilities for the rust helper functions.
///
use globset::{GlobBuilder, GlobMatcher};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// Global cache of compiled glob patterns
static GLOB_CACHE: Lazy<RwLock<HashMap<String, Arc<GlobMatcher>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Normalize a raw glob pattern into a recursive pattern similar to Python's rglob.
///
/// - "" or "*"  => None  (no filtering)
/// - "foo/*.nc" => "foo/*.nc"
/// - "*.nc"     => "**/*.nc"
pub fn normalize_pattern(raw: &str) -> Option<String> {
    if raw.is_empty() || raw == "*" {
        return None;
    }

    let norm = if raw.contains('/') || raw.contains('\\') {
        raw.to_string()
    } else {
        format!("**/{}", raw)
    };

    Some(norm.replace('\\', "/"))
}

/// Get (or build and cache) a GlobMatcher for a given raw glob pattern.
///
/// Returns:
/// - None      if the pattern is "" or "*"
/// - Some(Arc) if valid and compiled
pub fn get_glob_matcher(raw_pattern: &str) -> Option<Arc<GlobMatcher>> {
    let norm = match normalize_pattern(raw_pattern) {
        Some(n) => n,
        None => return None,
    };

    // Fast path: read lock
    {
        if let Ok(cache) = GLOB_CACHE.read() {
            if let Some(m) = cache.get(&norm) {
                return Some(m.clone());
            }
        }
    }

    // Slow path: compile
    let glob = match GlobBuilder::new(&norm).build() {
        Ok(g) => g,
        Err(_) => return None,
    };

    let matcher = Arc::new(glob.compile_matcher());

    // Insert into cache
    let mut cache = GLOB_CACHE.write().unwrap();
    let entry = cache.entry(norm).or_insert_with(|| matcher.clone());

    Some(entry.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to clear the global cache between tests.
    fn clear_cache() {
        let mut cache = GLOB_CACHE.write().unwrap();
        cache.clear();
    }

    #[test]
    fn normalize_pattern_handles_empty_and_star() {
        assert_eq!(normalize_pattern(""), None);
        assert_eq!(normalize_pattern("*"), None);
    }

    #[test]
    fn normalize_pattern_adds_recursive_prefix_for_simple_patterns() {
        assert_eq!(normalize_pattern("*.nc"), Some("**/*.nc".to_string()));
        assert_eq!(normalize_pattern("*.*"), Some("**/*.*".to_string()));
    }

    #[test]
    fn normalize_pattern_keeps_paths_with_separators() {
        assert_eq!(normalize_pattern("foo/*.nc"), Some("foo/*.nc".to_string()));
        // Backslashes are normalized to forward slashes
        assert_eq!(
            normalize_pattern(r"foo\bar\*.nc"),
            Some("foo/bar/*.nc".to_string())
        );
    }

    #[test]
    fn get_glob_matcher_returns_none_for_trivial_patterns() {
        clear_cache();
        assert!(get_glob_matcher("").is_none());
        assert!(get_glob_matcher("*").is_none());
    }

    #[test]
    fn get_glob_matcher_builds_and_matches_valid_patterns() {
        clear_cache();

        let matcher = get_glob_matcher("*.nc").expect("matcher should be built");

        // Pattern should have been normalized to "**/*.nc" and match both
        // top-level and nested .nc files.
        assert!(matcher.is_match("foo.nc"));
        assert!(matcher.is_match("subdir/foo.nc"));

        // Non-matching extensions should fail.
        assert!(!matcher.is_match("foo.txt"));
        assert!(!matcher.is_match("subdir/foo.txt"));
    }

    #[test]
    fn get_glob_matcher_uses_cache_for_same_pattern() {
        clear_cache();

        let m1 = get_glob_matcher("*.nc").expect("first matcher");
        let m2 = get_glob_matcher("*.nc").expect("second matcher");

        // Arc::ptr_eq checks that both Arcs point to the same allocation.
        assert!(Arc::ptr_eq(&m1, &m2));
    }

    #[test]
    fn get_glob_matcher_distinguishes_different_patterns() {
        clear_cache();

        let m_nc = get_glob_matcher("*.nc").expect("matcher for *.nc");
        let m_txt = get_glob_matcher("*.txt").expect("matcher for *.txt");

        assert!(!Arc::ptr_eq(&m_nc, &m_txt));

        assert!(m_nc.is_match("foo.nc"));
        assert!(!m_nc.is_match("foo.txt"));

        assert!(m_txt.is_match("foo.txt"));
        assert!(!m_txt.is_match("foo.nc"));
    }
}
