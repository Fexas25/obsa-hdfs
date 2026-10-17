use obs_rust_core::{ObsError, error::ObsResult};
use serde_json::Value;

pub(super) fn text<'a>(args: &'a Value, name: &str, nullable: bool) -> ObsResult<Option<&'a str>> {
    match args.get(name) {
        None => Ok(None),
        Some(Value::Null) if nullable => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        _ => Err(ObsError::InvalidArgument(format!("{name} must be a string"))),
    }
}

pub(super) fn boolean(args: &Value, name: &str, default: bool) -> ObsResult<bool> {
    match args.get(name) {
        None => Ok(default),
        Some(Value::Bool(value)) => Ok(*value),
        _ => Err(ObsError::InvalidArgument(format!("{name} must be a boolean"))),
    }
}

pub(super) fn permission(args: &Value, default: Option<u32>) -> ObsResult<u32> {
    match args.get("permission") {
        None => default.ok_or_else(|| ObsError::InvalidArgument("permission required".into())),
        Some(value) => value.as_u64().and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| ObsError::InvalidArgument("permission must be an unsigned 32-bit integer".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn supplied_fields_are_not_silently_defaulted_or_truncated() {
        for value in [Value::Null, json!(-1), json!(1.5), json!(4294967296u64), json!("644"), json!(true)] {
            assert!(permission(&json!({"permission": value}), Some(0o644)).is_err());
        }
        for value in [Value::Null, json!(0), json!("false"), json!([]), json!({})] {
            assert!(boolean(&json!({"overwrite": value}), "overwrite", false).is_err());
        }
        for value in [Value::Null, json!(1), json!(true), json!([]), json!({})] {
            assert!(text(&json!({"path": value}), "path", false).is_err());
        }
        assert!(text(&json!({"owner": 123}), "owner", true).is_err());
    }

    #[test]
    fn omitted_defaults_and_nullable_metadata_remain_supported() {
        // Kept separate from malformed supplied values for compatibility.
        assert_eq!(permission(&json!({}), Some(0o644)).unwrap(), 0o644);
        assert!(permission(&json!({}), None).is_err());
        assert_eq!(permission(&json!({"permission": u32::MAX}), None).unwrap(), u32::MAX);
        assert!(boolean(&json!({}), "create_parent", true).unwrap());
        assert!(!boolean(&json!({"create_parent": false}), "create_parent", true).unwrap());
        assert_eq!(text(&json!({}), "path", false).unwrap(), None);
        assert_eq!(text(&json!({"owner": null}), "owner", true).unwrap(), None);
        assert_eq!(text(&json!({"marker": ""}), "marker", true).unwrap(), Some(""));
    }
}
