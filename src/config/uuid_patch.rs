use std::collections::HashMap;

use anyhow::{Result, bail};
use uuid::Uuid;

/// Parses a `- name: <value>` line (the top-level list item marker for a
/// job entry), returning the indentation before the dash and the
/// unquoted name value. Returns `None` for any other line.
fn parse_dash_name_line(line: &str) -> Option<(&str, String)> {
    let trimmed = line.trim_start();
    let indent = &line[..line.len() - trimmed.len()];
    let after_dash = trimmed.strip_prefix('-')?.trim_start();
    let value = after_dash.strip_prefix("name:")?.trim();
    let value = value.trim_matches(|ch| ch == '"' || ch == '\'');
    Some((indent, value.to_string()))
}

/// Inserts a `uuid: <uuid>` line right after each job's `- name:` line for
/// the jobs listed in `missing`, matched by name. This assumes sundiald's own
/// generated/documented indentation style (a 2-space-indented `- name:`
/// followed by sibling keys indented 2 spaces further); if a name can't be
/// located, this fails loudly rather than silently writing a partial or
/// incorrect patch over the user's file.
pub(crate) fn insert_missing_job_uuids(raw: &str, missing: &[(String, Uuid)]) -> Result<String> {
    let mut remaining: HashMap<&str, Uuid> = missing
        .iter()
        .map(|(name, uuid)| (name.as_str(), *uuid))
        .collect();

    let mut output = Vec::with_capacity(raw.lines().count() + missing.len());
    for line in raw.lines() {
        output.push(line.to_string());
        if let Some((indent, name)) = parse_dash_name_line(line) {
            if let Some(uuid) = remaining.remove(name.as_str()) {
                output.push(format!("{indent}  uuid: {uuid}"));
            }
        }
    }

    if !remaining.is_empty() {
        let mut names: Vec<&str> = remaining.keys().copied().collect();
        names.sort_unstable();
        bail!(
            "could not locate job(s) {} in the config text to persist their generated uuid; \
             add `uuid: <uuid>` under each by hand",
            names.join(", ")
        );
    }

    let mut result = output.join("\n");
    if raw.ends_with('\n') {
        result.push('\n');
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserts_uuid_after_name_line_preserving_comments_and_indentation() {
        let raw = "jobs:\n  - name: heartbeat\n    # a comment that must survive\n    command: \"true\"\n";
        let id = Uuid::new_v4();

        let patched = insert_missing_job_uuids(raw, &[("heartbeat".to_string(), id)]).unwrap();

        assert_eq!(
            patched,
            format!(
                "jobs:\n  - name: heartbeat\n    uuid: {id}\n    # a comment that must survive\n    command: \"true\"\n"
            )
        );
    }

    #[test]
    fn only_patches_jobs_missing_a_uuid() {
        let raw = "jobs:\n  - name: alpha\n  - name: beta\n";
        let id = Uuid::new_v4();

        let patched = insert_missing_job_uuids(raw, &[("beta".to_string(), id)]).unwrap();

        assert_eq!(
            patched,
            format!("jobs:\n  - name: alpha\n  - name: beta\n    uuid: {id}\n")
        );
    }

    #[test]
    fn errors_without_writing_a_partial_patch_when_a_name_is_not_found() {
        let raw = "jobs:\n  - name: alpha\n";
        let id = Uuid::new_v4();

        let error = insert_missing_job_uuids(raw, &[("missing-job".to_string(), id)]).unwrap_err();

        assert!(error.to_string().contains("missing-job"));
    }
}
