use std::{
    collections::HashMap,
    env,
    ffi::OsString,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

pub(crate) type Environment = HashMap<String, OsString>;

pub(crate) fn environment() -> Environment {
    env::vars_os()
        .filter_map(|(key, value)| key.into_string().ok().map(|key| (key, value)))
        .collect()
}

pub(crate) fn user_directory(environment: &Environment) -> Option<PathBuf> {
    if let Some(path) = environment.get("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
        let path = PathBuf::from(path);
        if path.is_absolute() {
            return Some(path.join("sundiald"));
        }
    }
    environment
        .get("HOME")
        .filter(|v| !v.is_empty())
        .map(|home| PathBuf::from(home).join(".config/sundiald"))
}

pub(crate) fn expand_home(path: &Path, environment: &Environment) -> Result<PathBuf> {
    if path == Path::new("~") || path.starts_with("~/") {
        let home = environment
            .get("HOME")
            .context("HOME is required to expand ~/ in a path")?;
        return Ok(PathBuf::from(home).join(path.strip_prefix("~")?));
    }
    Ok(path.to_path_buf())
}

/// Select one file, never merge layers or hide errors in a selected file.
/// Explicit paths are returned even if missing, so the loader reports the error.
pub(crate) fn discover(
    explicit: Option<PathBuf>,
    environment: &Environment,
    filename: &str,
    system_directory: &Path,
) -> Result<Option<PathBuf>> {
    if let Some(path) = explicit {
        return expand_home(&path, environment).map(Some);
    }
    let paths = user_directory(environment)
        .map(|p| p.join(filename))
        .into_iter()
        .chain(std::iter::once(system_directory.join(filename)));
    for path in paths {
        if path
            .try_exists()
            .with_context(|| format!("failed to inspect config {}", path.display()))?
        {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn explicit_then_user_then_system_for_both_config_types() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let system = temp.path().join("etc/sundiald");
        let user = home.join(".config/sundiald");
        fs::create_dir_all(&system).unwrap();
        fs::create_dir_all(&user).unwrap();
        let env = HashMap::from([("HOME".into(), home.into_os_string())]);
        for filename in ["config.yaml", "client.yaml"] {
            assert!(discover(None, &env, filename, &system).unwrap().is_none());
            fs::write(system.join(filename), "{}").unwrap();
            assert_eq!(
                discover(None, &env, filename, &system).unwrap(),
                Some(system.join(filename))
            );
            fs::write(user.join(filename), "broken: [").unwrap();
            assert_eq!(
                discover(None, &env, filename, &system).unwrap(),
                Some(user.join(filename))
            );
            let explicit = temp.path().join("missing.yaml");
            assert_eq!(
                discover(Some(explicit.clone()), &env, filename, &system).unwrap(),
                Some(explicit)
            );
        }
    }

    #[test]
    fn xdg_replaces_home_and_missing_home_still_allows_system_config() {
        let temp = tempfile::tempdir().unwrap();
        let system = temp.path().join("etc");
        let xdg = temp.path().join("xdg");
        fs::create_dir_all(&system).unwrap();
        fs::create_dir_all(xdg.join("sundiald")).unwrap();
        fs::write(system.join("config.yaml"), "{}").unwrap();
        let mut env = HashMap::new();
        assert_eq!(
            discover(None, &env, "config.yaml", &system).unwrap(),
            Some(system.join("config.yaml"))
        );
        env.insert("XDG_CONFIG_HOME".into(), xdg.clone().into_os_string());
        assert_eq!(
            discover(None, &env, "config.yaml", &system).unwrap(),
            Some(system.join("config.yaml"))
        );
        fs::write(xdg.join("sundiald/config.yaml"), "{}").unwrap();
        assert_eq!(
            discover(None, &env, "config.yaml", &system).unwrap(),
            Some(xdg.join("sundiald/config.yaml"))
        );
        env.insert("XDG_CONFIG_HOME".into(), "relative".into());
        assert_eq!(
            discover(None, &env, "config.yaml", &system).unwrap(),
            Some(system.join("config.yaml"))
        );
    }
}
