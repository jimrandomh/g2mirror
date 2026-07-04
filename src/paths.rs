//! The ~/.g2mirror runtime directory: session sockets and server config.

use std::os::unix::fs::{FileTypeExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

/// How old a socket file must be before the server will consider removing it
/// (its owning PID must also be gone).
pub const STALE_SOCKET_AGE: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// Runtime directory (default `~/.g2mirror`, overridable with $G2MIRROR_DIR
/// for tests). Created if missing; permissions forced to 700 either way.
pub fn g2mirror_dir() -> std::io::Result<PathBuf> {
    let dir = match std::env::var_os("G2MIRROR_DIR") {
        Some(d) => PathBuf::from(d),
        None => {
            let home = std::env::var_os("HOME").ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "HOME is not set")
            })?;
            PathBuf::from(home).join(".g2mirror")
        }
    };
    std::fs::create_dir_all(&dir)?;
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
    Ok(dir)
}

pub fn config_path(dir: &Path) -> PathBuf {
    dir.join("config.json")
}

/// Session socket file name: `<pid>-<sanitized-cwd>`.
pub fn socket_name(pid: u32, cwd: &Path) -> String {
    format!("{pid}-{}", sanitize_cwd(cwd))
}

/// Parse the PID prefix out of a session socket file name.
pub fn socket_pid(name: &str) -> Option<u32> {
    name.split('-').next()?.parse().ok()
}

/// A socket name is a single path component of the characters
/// `sanitize_cwd` can produce, with a numeric PID prefix.
pub fn is_valid_socket_name(name: &str) -> bool {
    socket_pid(name).is_some()
        && !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// Replace characters that are awkward in file names and truncate, keeping
/// the tail of the path (the most distinctive part). The limit keeps the
/// whole socket path under the ~104-byte sun_path limit.
fn sanitize_cwd(cwd: &Path) -> String {
    let s = cwd.to_string_lossy();
    let sanitized: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let max = 60;
    if sanitized.len() > max {
        sanitized[sanitized.len() - max..].to_string()
    } else {
        sanitized
    }
}

/// Remove session sockets whose file timestamp is old and whose owning PID
/// no longer exists. Both conditions are required: a fresh file might belong
/// to a wrapper that just crashed and whose PID was reused, and a live PID
/// means the session is still running no matter how old the file is.
pub fn cleanup_stale_sockets(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut removed = Vec::new();
    let now = std::time::SystemTime::now();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(pid) = socket_pid(name) else { continue };
        let Ok(meta) = entry.metadata() else { continue };
        if !meta.file_type().is_socket() {
            continue;
        }
        let old = meta
            .modified()
            .ok()
            .and_then(|m| now.duration_since(m).ok())
            .is_some_and(|age| age > STALE_SOCKET_AGE);
        if old && !pid_exists(pid)
            && std::fs::remove_file(entry.path()).is_ok() {
                removed.push(entry.path());
            }
    }
    Ok(removed)
}

pub fn pid_exists(pid: u32) -> bool {
    let Ok(raw) = i32::try_from(pid) else {
        return false;
    };
    let Some(pid) = rustix::process::Pid::from_raw(raw) else {
        return false;
    };
    match rustix::process::test_kill_process(pid) {
        Ok(()) => true,
        // EPERM means the process exists but belongs to someone else.
        Err(rustix::io::Errno::PERM) => true,
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socket_names_are_sanitized_and_bounded() {
        let name = socket_name(1234, Path::new("/Users/jim/my project/x"));
        assert_eq!(name, "1234-_Users_jim_my_project_x");
        assert!(is_valid_socket_name(&name));
        assert_eq!(socket_pid(&name), Some(1234));

        let long = "/a".repeat(200);
        let name = socket_name(1, Path::new(&long));
        assert!(name.len() <= 60 + 8);
        assert!(is_valid_socket_name(&name));
    }

    #[test]
    fn rejects_path_traversal_in_socket_names() {
        assert!(!is_valid_socket_name("123-../../etc/passwd"));
        assert!(!is_valid_socket_name("../123-x"));
        assert!(!is_valid_socket_name("nopid"));
        assert!(!is_valid_socket_name(""));
    }

    #[test]
    fn pid_liveness() {
        assert!(pid_exists(std::process::id()));
        assert!(!pid_exists(0x7fff_fff0)); // far beyond any real pid_max
    }
}
