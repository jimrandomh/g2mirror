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

/// Set in the wrapped command's environment to the session socket's path,
/// so a nested `g2mirror <command>` (e.g. from a shell alias) can tell it
/// is already being mirrored and run the command unwrapped.
pub const SESSION_ENV: &str = "G2MIRROR_SESSION";

/// The session this process is running inside, per `$G2MIRROR_SESSION` —
/// only if that socket still exists, so a stale value (the wrapper exited
/// but a descendant, e.g. a tmux server, lives on) doesn't suppress
/// wrapping forever.
pub fn enclosing_session() -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os(SESSION_ENV)?);
    std::fs::symlink_metadata(&path)
        .is_ok_and(|m| m.file_type().is_socket())
        .then_some(path)
}

pub fn config_path(dir: &Path) -> PathBuf {
    dir.join("config.json")
}

/// Longest socket path `bind` accepts: `sun_path` minus its NUL.
#[cfg(any(target_os = "linux", target_os = "android"))]
const MAX_SOCKET_PATH: usize = 107;
#[cfg(not(any(target_os = "linux", target_os = "android")))]
const MAX_SOCKET_PATH: usize = 103;

/// Longest sanitized cwd kept in a socket name, even when there's room.
const MAX_CWD_IN_NAME: usize = 60;

/// Session socket path in `dir`: `<pid>-<sanitized-cwd>`, with the cwd part
/// cut down (keeping its tail) as far as needed for the whole path to fit
/// `sun_path` — down to a bare `<pid>` under a very long `$G2MIRROR_DIR`.
pub fn socket_path(dir: &Path, pid: u32, cwd: &Path) -> PathBuf {
    let room = MAX_SOCKET_PATH
        .saturating_sub(dir.as_os_str().len() + 1)
        .saturating_sub(pid.to_string().len() + 1);
    dir.join(socket_name(pid, cwd, room.min(MAX_CWD_IN_NAME)))
}

/// Session socket file name: `<pid>-<sanitized-cwd>`, the cwd part at most
/// `max_cwd` bytes (a bare `<pid>` when that's 0).
fn socket_name(pid: u32, cwd: &Path, max_cwd: usize) -> String {
    let cwd = sanitize_cwd(cwd, max_cwd);
    if cwd.is_empty() {
        pid.to_string()
    } else {
        format!("{pid}-{cwd}")
    }
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

/// Replace characters that are awkward in file names and truncate to `max`
/// bytes, keeping the tail of the path (the most distinctive part).
fn sanitize_cwd(cwd: &Path, max: usize) -> String {
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
        let home = Path::new("/home/jim/.g2mirror");
        let path = socket_path(home, 1234, Path::new("/Users/jim/my project/x"));
        let name = path.file_name().unwrap().to_str().unwrap();
        assert_eq!(name, "1234-_Users_jim_my_project_x");
        assert!(is_valid_socket_name(name));
        assert_eq!(socket_pid(name), Some(1234));

        let long = "/a".repeat(200);
        let path = socket_path(home, 1, Path::new(&long));
        let name = path.file_name().unwrap().to_str().unwrap();
        assert_eq!(name.len(), MAX_CWD_IN_NAME + 2);
        assert!(is_valid_socket_name(name));
    }

    #[test]
    fn socket_paths_fit_under_long_runtime_dirs() {
        let cwd = Path::new("/home/jim/repositories/some-project/src");
        for dir_len in [20, 60, 90, 100, 104, 150] {
            let dir = PathBuf::from(format!("/{}", "d".repeat(dir_len - 1)));
            let path = socket_path(&dir, 4_194_304, cwd);
            let name = path.file_name().unwrap().to_str().unwrap();
            assert!(is_valid_socket_name(name), "{name}");
            assert_eq!(socket_pid(name), Some(4_194_304));
            // Fits whenever the pid alone can; the tail of the cwd is kept.
            if dir_len + 1 + 7 <= MAX_SOCKET_PATH {
                let len = path.as_os_str().len();
                assert!(len <= MAX_SOCKET_PATH, "{}", path.display());
            }
            if name.len() > 8 {
                assert!("_home_jim_repositories_some-project_src".ends_with(&name[8..]));
            }
        }
        // A real bind at the limit succeeds.
        let tmp = std::env::temp_dir().join(format!("g2m-{}", std::process::id()));
        let dir = tmp.join("x".repeat(MAX_SOCKET_PATH - tmp.as_os_str().len() - 30));
        std::fs::create_dir_all(&dir).unwrap();
        let path = socket_path(&dir, std::process::id(), cwd);
        let bound = std::os::unix::net::UnixListener::bind(&path);
        let _ = std::fs::remove_dir_all(&tmp);
        bound.expect("socket path should fit sun_path");
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
