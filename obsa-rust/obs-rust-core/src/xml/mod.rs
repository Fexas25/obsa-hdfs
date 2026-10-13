use quick_xml::events::Event;
use quick_xml::Reader;

use crate::error::{ObsError, ObsResult};
use crate::types::ListEntry;

/// Reject incomplete, wrong-root and multi-document success responses. A
/// syntactically plausible prefix must not authorize destructive follow-up work.
pub fn validate_document(body: &str, root: &str) -> ObsResult<()> {
    let mut reader = Reader::from_str(body);
    let mut depth = 0usize;
    let mut seen = false;
    let invalid = || ObsError::Io(format!("invalid or incomplete {root} XML response"));
    loop {
        match reader.read_event().map_err(|_| invalid())? {
            Event::Start(e) if depth == 0 => {
                if seen || e.name().as_ref() != root.as_bytes() {
                    return Err(invalid());
                }
                seen = true;
                depth = 1;
            }
            Event::Empty(e) if depth == 0 => {
                if seen || e.name().as_ref() != root.as_bytes() {
                    return Err(invalid());
                }
                seen = true;
            }
            Event::Start(_) => {
                depth += 1;
                if depth > 64 {
                    return Err(invalid());
                }
            }
            Event::End(_) => {
                depth = depth.checked_sub(1).ok_or_else(invalid)?;
            }
            Event::Text(t) => {
                let text = t.unescape().map_err(|_| invalid())?;
                if depth == 0 && !text.trim().is_empty() {
                    return Err(invalid());
                }
            }
            Event::CData(_) if depth == 0 => return Err(invalid()),
            Event::DocType(_) => return Err(invalid()),
            Event::Eof => {
                return if seen && depth == 0 {
                    Ok(())
                } else {
                    Err(invalid())
                }
            }
            _ => {}
        }
    }
}

/// Parse OBS XML error response preserving error code.
pub fn parse_error_xml(body: &str) -> (String, String) {
    let mut reader = Reader::from_str(body);
    reader.config_mut().trim_text(false);

    let mut code = String::new();
    let mut message = String::new();
    let mut depth = 0usize;
    let mut active: Option<(bool, usize)> = None;
    let mut invalid_field_depth = None;
    let mut field_text = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                depth += 1;
                if invalid_field_depth.is_some() { continue; }
                // Nested markup invalidates the scalar instead of allowing its
                // prefix or child text to become a control-flow error code.
                if let Some((_, field_depth)) = active.take() {
                    invalid_field_depth = Some(field_depth);
                    field_text.clear();
                    continue;
                }
                match e.name().as_ref() {
                    b"Code" => active = Some((true, depth)),
                    b"Message" => active = Some((false, depth)),
                    _ => continue,
                }
                field_text.clear();
            }
            Ok(Event::Empty(_)) => {
                if let Some((_, field_depth)) = active {
                    invalid_field_depth = Some(field_depth);
                }
                active = None;
                field_text.clear();
            }
            Ok(Event::Text(e)) if active.is_some() => {
                match e.unescape() {
                    Ok(text) => field_text.push_str(&text),
                    Err(_) => { active = None; field_text.clear(); }
                }
            }
            Ok(Event::CData(e)) if active.is_some() => {
                match std::str::from_utf8(e.as_ref()) {
                    Ok(text) => field_text.push_str(text),
                    Err(_) => { active = None; field_text.clear(); }
                }
            }
            Ok(Event::End(_)) => {
                if invalid_field_depth == Some(depth) { invalid_field_depth = None; }
                if let Some((is_code, field_depth)) = active {
                    if field_depth == depth {
                        let text = field_text.trim();
                        if !text.is_empty() {
                            if is_code { code = text.to_owned(); }
                            else { message = text.to_owned(); }
                        }
                        active = None;
                        field_text.clear();
                    }
                }
                depth = depth.saturating_sub(1);
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
    }

    (code, message)
}

// Track only fields consumed by this parser, independently for the root,
// current object and current common-prefix record. Unknown metadata is ignored.
fn claim_list_field(name: &str, scope: usize, depth: usize, seen: &mut [u16; 3]) -> ObsResult<bool> {
    let bit = match (scope, name) {
        (0, "IsTruncated") | (1, "Key") | (2, "Prefix") => 1,
        (0, "NextMarker") | (1, "Size") => 2,
        (1, "ETag") => 4,
        (1, "LastModified") => 8,
        (1, "Mode") => 16,
        (1, "Uid") => 32,
        (1, "Gid") => 64,
        (1, "InodeNo") => 128,
        (1, "FsDentryNo") => 256,
        (1, "FsLinkCount") => 512,
        _ => return Ok(false),
    };
    if depth != if scope == 0 { 2 } else { 3 } {
        return Err(ObsError::Io(format!("misplaced LIST field: {name}")));
    }
    if seen[scope] & bit != 0 {
        return Err(ObsError::Io(format!("duplicate LIST field: {name}")));
    }
    seen[scope] |= bit;
    Ok(true)
}

/// Parse ListBucketResult XML including PFS fields (Mode/Gid/Uid/InodeNo).
pub fn parse_list_xml(body: &str) -> ObsResult<ListXmlResult> {
    validate_document(body, "ListBucketResult")?;
    let mut reader = Reader::from_str(body);
    // Object names and continuation markers are opaque values. Whitespace is
    // significant there; only typed scalar fields below may be trimmed.
    reader.config_mut().trim_text(false);

    let mut result = ListXmlResult::default();
    let mut current: Option<ListEntry> = None;
    let mut current_prefix: Option<String> = None;
    let mut field = String::new();
    let mut field_text = String::new();
    let mut seen_fields = [0u16; 3];
    let mut value_open = false;
    let mut depth = 0usize;

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                if value_open {
                    return Err(ObsError::Io("nested element in LIST scalar field".into()));
                }
                depth += 1;
                field_text.clear();
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                match name.as_str() {
                    "Contents" | "CommonPrefixes" => {
                        if depth != 2 || current.is_some() || current_prefix.is_some() {
                            return Err(ObsError::Io("misplaced LIST entry container".into()));
                        }
                        field.clear();
                        if name == "Contents" {
                            seen_fields[1] = 0;
                            current = Some(ListEntry::default());
                        } else {
                            seen_fields[2] = 0;
                            current_prefix = Some(String::new());
                        }
                    }
                    other => {
                        let scope = if current.is_some() { 1 } else if current_prefix.is_some() { 2 } else { 0 };
                        value_open = claim_list_field(other, scope, depth, &mut seen_fields)?;
                        field = other.to_string();
                    }
                }
            }
            Ok(Event::Empty(e)) => {
                if value_open {
                    return Err(ObsError::Io("nested element in LIST scalar field".into()));
                }
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if matches!(name.as_str(), "Contents" | "CommonPrefixes") && (depth != 1 || current.is_some() || current_prefix.is_some()) {
                    return Err(ObsError::Io("misplaced LIST entry container".into()));
                }
                let scope = if current.is_some() { 1 } else if current_prefix.is_some() { 2 } else { 0 };
                claim_list_field(&name, scope, depth + 1, &mut seen_fields)?;
            }
            Ok(Event::Text(e)) => {
                if !field.is_empty() {
                    let text = e.unescape().map_err(|error| ObsError::Io(format!("XML text error: {error}")))?;
                    field_text.push_str(&text);
                }
            }
            Ok(Event::CData(e)) => {
                if !field.is_empty() {
                    // CDATA is literal text: do not decode its ampersands again.
                    let text = std::str::from_utf8(e.as_ref())
                        .map_err(|_| ObsError::Io("invalid LIST CDATA encoding".into()))?;
                    field_text.push_str(text);
                }
            }
            Ok(Event::End(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                // XML comments and CDATA may split one field into multiple
                // events. Parse its complete value only at the closing tag.
                if name == field && !field_text.is_empty() {
                    let text = std::mem::take(&mut field_text);
                    if let Some(ref mut entry) = current {
                        match field.as_str() {
                            "Key" => entry.key = text,
                            "Size" => {
                                entry.size = text
                                    .trim()
                                    .parse()
                                    .map_err(|_| ObsError::Io("invalid LIST object size".into()))?
                            }
                            "ETag" => entry.etag = Some(text.trim().trim_matches('"').to_string()),
                            "LastModified" => entry.last_modified = Some(text.trim().to_string()),
                            "Mode" => entry.mode = ListEntry::parse_mode_octal(text.trim()),
                            "Uid" => entry.uid = text.trim().parse().ok(),
                            "Gid" => entry.gid = text.trim().parse().ok(),
                            "InodeNo" => entry.inode_no = text.trim().parse().ok(),
                            "FsDentryNo" => entry.fs_dentry_no = text.trim().parse().ok(),
                            "FsLinkCount" => entry.fs_link_count = text.trim().parse().ok(),
                            _ => {}
                        }
                    } else if let Some(ref mut prefix) = current_prefix {
                        if field == "Prefix" {
                            *prefix = text;
                        }
                    } else {
                        match field.as_str() {
                            "IsTruncated" => {
                                result.is_truncated = match text.trim() {
                                    "true" | "1" => true,
                                    "false" | "0" => false,
                                    _ => return Err(ObsError::Io("invalid LIST IsTruncated".into())),
                                }
                            }
                            "NextMarker" => result.next_marker = Some(text),
                            _ => {}
                        }
                    }
                }
                match name.as_str() {
                    "Contents" => {
                        if let Some(entry) = current.take() {
                            result.contents.push(entry);
                        }
                    }
                    "CommonPrefixes" => {
                        if let Some(prefix) = current_prefix.take() {
                            result.common_prefixes.push(prefix.clone());
                            result.contents.push(ListEntry {
                                key: prefix,
                                is_common_prefix: true,
                                ..Default::default()
                            });
                        }
                    }
                    _ => {}
                }
                field.clear();
                value_open = false;
                depth -= 1; // The document validator guarantees balanced tags.
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(ObsError::Io(format!("XML parse error: {e}"))),
            _ => {}
        }
    }

    Ok(result)
}

#[derive(Debug, Default)]
pub struct ListXmlResult {
    pub contents: Vec<ListEntry>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_marker: Option<String>,
}

/// Parse InitiateMultipartUploadResult.
pub fn parse_initiate_mpu_xml(body: &str) -> ObsResult<String> {
    validate_document(body, "InitiateMultipartUploadResult")?;
    let mut reader = Reader::from_str(body);
    reader.config_mut().trim_text(false);
    let mut upload_id = String::new();
    let mut in_upload_id = false;
    let mut seen_upload_id = false;
    let mut depth = 0usize;

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                if in_upload_id {
                    return Err(ObsError::Io("nested element in UploadId".into()));
                }
                depth += 1;
                if e.name().as_ref() == b"UploadId" {
                    if depth != 2 || seen_upload_id {
                        return Err(ObsError::Io("duplicate or misplaced UploadId".into()));
                    }
                    seen_upload_id = true;
                    in_upload_id = true;
                }
            }
            Ok(Event::Empty(e)) => {
                if in_upload_id {
                    return Err(ObsError::Io("nested element in UploadId".into()));
                }
                if e.name().as_ref() == b"UploadId" {
                    if depth != 1 || seen_upload_id {
                        return Err(ObsError::Io("duplicate or misplaced UploadId".into()));
                    }
                    seen_upload_id = true;
                }
            }
            Ok(Event::Text(e)) if in_upload_id => {
                let text = e.unescape().map_err(|error| ObsError::Io(format!("XML text error: {error}")))?;
                upload_id.push_str(&text);
            }
            Ok(Event::CData(e)) if in_upload_id => {
                let text = std::str::from_utf8(e.as_ref())
                    .map_err(|_| ObsError::Io("invalid UploadId CDATA encoding".into()))?;
                upload_id.push_str(text);
            }
            Ok(Event::End(_)) => {
                // Document validation guarantees balanced tags; nested elements
                // in the scalar were rejected before entering this branch.
                in_upload_id = false;
                depth -= 1;
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(ObsError::Io(format!("XML parse error: {e}"))),
            _ => {}
        }
    }

    let upload_id = upload_id.trim();
    if upload_id.is_empty() {
        return Err(ObsError::Io("missing UploadId".into()));
    }
    Ok(upload_id.to_owned())
}

/// Parse a complete MPU or copy result, preserving the public Option API.
pub fn parse_complete_mpu_etag(body: &str) -> Option<String> {
    parse_result_etag(body, "CompleteMultipartUploadResult")
        .or_else(|_| parse_result_etag(body, "CopyObjectResult"))
        .ok()
}

/// Store callers supply their expected root so a result from another operation
/// cannot be mistaken for success. Validate once, then consume the full field.
pub(crate) fn parse_result_etag(body: &str, root: &str) -> ObsResult<String> {
    validate_document(body, root)?;
    let mut reader = Reader::from_str(body);
    reader.config_mut().trim_text(false);
    let mut in_etag = false;
    let mut seen_etag = false;
    let mut depth = 0usize;
    let mut etag = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                if in_etag {
                    return Err(ObsError::Io("nested element in result ETag".into()));
                }
                depth += 1;
                if e.name().as_ref() == b"ETag" {
                    if depth != 2 || seen_etag {
                        return Err(ObsError::Io("duplicate or misplaced result ETag".into()));
                    }
                    seen_etag = true;
                    in_etag = true;
                }
            }
            Ok(Event::Empty(e)) => {
                if in_etag {
                    return Err(ObsError::Io("nested element in result ETag".into()));
                }
                if e.name().as_ref() == b"ETag" {
                    if depth != 1 || seen_etag {
                        return Err(ObsError::Io("duplicate or misplaced result ETag".into()));
                    }
                    seen_etag = true;
                }
            }
            Ok(Event::Text(e)) if in_etag => {
                let text = e.unescape().map_err(|error| ObsError::Io(format!("XML text error: {error}")))?;
                etag.push_str(&text);
            }
            Ok(Event::CData(e)) if in_etag => {
                let text = std::str::from_utf8(e.as_ref())
                    .map_err(|_| ObsError::Io("invalid ETag CDATA encoding".into()))?;
                etag.push_str(text);
            }
            Ok(Event::End(_)) => {
                in_etag = false;
                depth -= 1; // validate_document guarantees balanced tags.
            }
            Ok(Event::Eof) => break,
            Err(error) => return Err(ObsError::Io(format!("XML parse error: {error}"))),
            _ => {}
        }
    }
    let value = etag.trim();
    let value = if value.starts_with('"') || value.ends_with('"') {
        value.strip_prefix('"').and_then(|value| value.strip_suffix('"'))
            .ok_or_else(|| ObsError::Io("unbalanced quotes in result ETag".into()))?
    } else { value };
    if value.trim().is_empty() {
        return Err(ObsError::Io("missing or empty result ETag".into()));
    }
    Ok(value.to_owned())
}

/// Quiet deletion succeeds only with a complete DeleteResult and no Error
/// elements. An empty/truncated response is not evidence that every key won.
pub fn delete_result_has_errors(body: &str) -> ObsResult<bool> {
    validate_document(body, "DeleteResult")?;
    let mut reader = Reader::from_str(body);
    loop {
        match reader
            .read_event()
            .map_err(|e| ObsError::Io(e.to_string()))?
        {
            Event::Start(e) | Event::Empty(e) if e.local_name().as_ref() == b"Error" => {
                return Ok(true)
            }
            Event::Eof => return Ok(false),
            _ => {}
        }
    }
}

/// Build batch delete XML body.
pub fn build_delete_xml(keys: &[&str]) -> String {
    let objects: String = keys
        .iter()
        .map(|k| format!("<Object><Key>{}</Key></Object>", xml_escape(k)))
        .collect();
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete>{objects}<Quiet>true</Quiet></Delete>"
    )
}

/// Build complete MPU XML.
pub fn build_complete_mpu_xml(parts: &[(i32, &str)]) -> String {
    let part_xml: String = parts
        .iter()
        .map(|(num, etag)| {
            format!(
                "<Part><PartNumber>{num}</PartNumber><ETag>\"{}\"</ETag></Part>",
                xml_escape(etag)
            )
        })
        .collect();
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUpload>{part_xml}</CompleteMultipartUpload>"
    )
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        // Literal CR is normalized to LF by XML parsers; a reference preserves it.
        .replace('\r', "&#13;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_rejects_consumed_fields_hidden_in_extensions() {
        for content in [
            "<Contents><Extension><Key>wrong</Key></Extension></Contents>",
            "<Contents><Key>right</Key><Extension><Size>99</Size></Extension></Contents>",
            "<Extension><IsTruncated>false</IsTruncated></Extension>",
            "<Extension><NextMarker/></Extension>",
            "<CommonPrefixes><Extension><Prefix>wrong/</Prefix></Extension></CommonPrefixes>",
            "<Extension><Contents><Key>wrong</Key></Contents></Extension>",
            "<Extension><CommonPrefixes/></Extension>",
        ] {
            assert!(parse_list_xml(&format!("<ListBucketResult>{content}</ListBucketResult>")).is_err(), "{content}");
        }
        let page = parse_list_xml("<ListBucketResult><Prefix>dir/</Prefix><Extension><Label>ignored</Label></Extension><Contents><Key>dir/right</Key><Owner><ID>owner</ID></Owner><Extension><Label>ignored</Label></Extension><Size>7</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>").unwrap();
        assert_eq!(page.contents[0].key, "dir/right");
        assert_eq!(page.contents[0].size, 7);
    }

    #[test]
    fn error_fields_assemble_text_comments_and_literal_cdata() {
        assert_eq!(parse_error_xml("<Error><Code> Directory<!--split--><![CDATA[NotEmpty]]> </Code><Message> a &amp; <!--split--><![CDATA[b &amp;]]> c </Message></Error>"),
            ("DirectoryNotEmpty".into(), "a & b &amp; c".into()));
        assert_eq!(parse_error_xml("<Error><Code><![CDATA[CopyFailed]]></Code><Message><![CDATA[failed]]></Message></Error>"),
            ("CopyFailed".into(), "failed".into()));
    }

    #[test]
    fn error_fields_do_not_capture_siblings_or_partial_values() {
        assert_eq!(parse_error_xml("<Error><Code></Code><Message>details</Message></Error>"),
            (String::new(), "details".into()));
        assert_eq!(parse_error_xml("<Error><Code/><Message>details</Message></Error>"),
            (String::new(), "details".into()));
        assert_eq!(parse_error_xml("<Error><Code>unfinished"), (String::new(), String::new()));
        assert_eq!(parse_error_xml("<Error><Code>a<Extra/>b</Code><Message>details</Message></Error>"),
            (String::new(), "details".into()));
        assert_eq!(parse_error_xml("<Error><Code>a<Extra><Code>forged</Code></Extra>b</Code><Message>details</Message></Error>"),
            (String::new(), "details".into()));
    }

    #[test]
    fn result_etag_assembles_all_text_and_literal_cdata() {
        for root in ["CompleteMultipartUploadResult", "CopyObjectResult"] {
            for (field, expected) in [
                (" &quot;ab<!--split--><![CDATA[&amp;]]>cd&quot; ", "ab&amp;cd"),
                ("<![CDATA[\"only-cdata\"]]>", "only-cdata"),
                ("plain&amp;tag", "plain&tag"),
            ] {
                assert_eq!(parse_complete_mpu_etag(&format!("<{root}><ETag>{field}</ETag></{root}>")), Some(expected.into()));
            }
        }
    }

    #[test]
    fn result_etag_rejects_ambiguous_nested_and_incomplete_values() {
        for root in ["CompleteMultipartUploadResult", "CopyObjectResult"] {
            for fields in ["<ETag>a</ETag><ETag>b</ETag>", "<ETag/><ETag>b</ETag>",
                "<ETag>a</ETag><ETag/>", "<ETag>a<Extra/>b</ETag>",
                "<Extra><ETag>a</ETag></Extra>", "<ETag>&quot; </ETag>",
                "<ETag>&quot; &quot;</ETag>", "<ETag><![CDATA[ ]]></ETag>", "<ETag/>"] {
                assert!(parse_complete_mpu_etag(&format!("<{root}>{fields}</{root}>")).is_none(), "{fields}");
            }
            assert!(parse_complete_mpu_etag(&format!("<{root}><ETag>a</ETag>")).is_none());
        }
        assert!(parse_complete_mpu_etag("<Other><ETag>a</ETag></Other>").is_none());
    }

    #[test]
    fn multipart_upload_id_assembles_complete_literal_value() {
        assert_eq!(parse_initiate_mpu_xml("<InitiateMultipartUploadResult><UploadId> up<!--split--><![CDATA[&amp;]]>load </UploadId></InitiateMultipartUploadResult>").unwrap(), "up&amp;load");
        assert_eq!(parse_initiate_mpu_xml("<InitiateMultipartUploadResult><UploadId><![CDATA[only-cdata]]></UploadId></InitiateMultipartUploadResult>").unwrap(), "only-cdata");
    }

    #[test]
    fn multipart_upload_id_rejects_ambiguous_or_misplaced_values() {
        for content in ["<UploadId>a</UploadId><UploadId>b</UploadId>",
            "<UploadId/><UploadId>b</UploadId>", "<UploadId>a</UploadId><UploadId/>",
            "<UploadId>a<Extra/>b</UploadId>", "<UploadId><Extra>b</Extra></UploadId>",
            "<Extra><UploadId>b</UploadId></Extra>", "<UploadId><![CDATA[ ]]></UploadId>"] {
            assert!(parse_initiate_mpu_xml(&format!("<InitiateMultipartUploadResult>{content}</InitiateMultipartUploadResult>")).is_err(), "{content}");
        }
    }

    #[test]
    fn list_rejects_repeated_consumed_fields_including_empty_elements() {
        for fields in ["<Key>a</Key><Key>b</Key>", "<Key/><Key>b</Key>",
            "<Key>a</Key><Key/>", "<Size>1</Size><Size>2</Size>",
            "<Mode>100644</Mode><Mode>040755</Mode>", "<ETag>a</ETag><ETag>a</ETag>"] {
            assert!(parse_list_xml(&format!("<ListBucketResult><Contents>{fields}</Contents></ListBucketResult>")).is_err(), "{fields}");
        }
        for fields in ["<IsTruncated>true</IsTruncated><IsTruncated>false</IsTruncated>",
            "<NextMarker>a</NextMarker><NextMarker>b</NextMarker>",
            "<CommonPrefixes><Prefix>a/</Prefix><Prefix>b/</Prefix></CommonPrefixes>"] {
            assert!(parse_list_xml(&format!("<ListBucketResult>{fields}</ListBucketResult>")).is_err(), "{fields}");
        }
        // Per-entry tracking resets for the next object and directory prefix.
        let page = parse_list_xml("<ListBucketResult><Contents><Key>a</Key></Contents><Contents><Key>b</Key></Contents><CommonPrefixes><Prefix>c/</Prefix></CommonPrefixes><CommonPrefixes><Prefix>d/</Prefix></CommonPrefixes></ListBucketResult>").unwrap();
        assert_eq!(page.contents.len(), 4);
    }

    #[test]
    fn list_rejects_nested_values_and_overlapping_entry_containers() {
        for fields in ["<Contents><Key>a<Unknown/>b</Key></Contents>",
            "<Contents><Size><Unknown>12</Unknown></Size></Contents>",
            "<NextMarker>a<Unknown>b</Unknown></NextMarker>",
            "<Contents><Key>a</Key><Contents><Key>b</Key></Contents></Contents>",
            "<Contents><CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes></Contents>"] {
            assert!(parse_list_xml(&format!("<ListBucketResult>{fields}</ListBucketResult>")).is_err(), "{fields}");
        }
        let page = parse_list_xml("<ListBucketResult><Contents><Key>a</Key><Owner><ID>owner-id</ID><DisplayName>name</DisplayName></Owner></Contents></ListBucketResult>").unwrap();
        assert_eq!(page.contents[0].key, "a");
    }

    #[test]
    fn list_assembles_text_and_cdata_before_parsing_fields() {
        let result = parse_list_xml(r#"<ListBucketResult>
          <Contents><Key> dir/<!--split--><![CDATA[file &amp;]]> </Key>
            <Size> 1<!--split-->2 </Size><Mode>100<![CDATA[644]]></Mode>
            <Uid>4<!--split-->2</Uid><ETag>&quot;ta<![CDATA[g]]>&quot;</ETag>
          </Contents>
          <CommonPrefixes><Prefix> dir/<![CDATA[sub &]]>/ </Prefix></CommonPrefixes>
          <IsTruncated> tr<!--split--><![CDATA[ue]]> </IsTruncated>
          <NextMarker> marker<!--split--><![CDATA[ &amp;]]> </NextMarker>
        </ListBucketResult>"#).unwrap();
        assert_eq!(result.contents[0].key, " dir/file &amp; ");
        assert_eq!(result.contents[0].size, 12);
        assert_eq!(result.contents[0].mode, Some(0o100644));
        assert_eq!(result.contents[0].uid, Some(42));
        assert_eq!(result.contents[0].etag.as_deref(), Some("tag"));
        assert_eq!(result.common_prefixes, vec![" dir/sub &/ "]);
        assert!(result.is_truncated);
        assert_eq!(result.next_marker.as_deref(), Some(" marker &amp; "));
    }

    #[test]
    fn list_preserves_key_prefix_and_marker_whitespace() {
        for key in [" leading", "trailing ", " both ", "   ", " dir/文件 "] {
            let result = parse_list_xml(&format!(
                "<ListBucketResult><Contents><Key>{key}</Key><Size> 7 </Size><Mode> 100644 </Mode><Uid> 42 </Uid><Gid> 43 </Gid><InodeNo> 44 </InodeNo><FsDentryNo> 45 </FsDentryNo><FsLinkCount> 2 </FsLinkCount><ETag> &quot;tag&quot; </ETag></Contents><CommonPrefixes><Prefix>{key}/ </Prefix></CommonPrefixes><IsTruncated> true </IsTruncated><NextMarker>{key}</NextMarker></ListBucketResult>"
            )).unwrap();
            let entry = &result.contents[0];
            assert_eq!(entry.key, key);
            assert_eq!(entry.size, 7);
            assert_eq!(entry.mode, Some(0o100644));
            assert_eq!((entry.uid, entry.gid), (Some(42), Some(43)));
            assert_eq!((entry.inode_no, entry.fs_dentry_no, entry.fs_link_count), (Some(44), Some(45), Some(2)));
            assert_eq!(entry.etag.as_deref(), Some("tag"));
            assert_eq!(result.common_prefixes, vec![format!("{key}/ ")]);
            assert_eq!(result.next_marker.as_deref(), Some(key));
            assert!(result.is_truncated);
        }
    }

    #[test]
    fn parse_list_with_pfs_fields() {
        let xml = r#"<?xml version="1.0"?>
        <ListBucketResult>
            <Contents>
                <Key>dir/file</Key>
                <Size>1024</Size>
                <Mode>100644</Mode>
                <Uid>1000</Uid>
                <Gid>1000</Gid>
                <InodeNo>12345</InodeNo>
            </Contents>
            <IsTruncated>false</IsTruncated>
        </ListBucketResult>"#;

        let result = parse_list_xml(xml).unwrap();
        assert_eq!(result.contents.len(), 1);
        assert_eq!(result.contents[0].inode_no, Some(12345));
        assert!(result.contents[0].is_file());
    }
}

#[cfg(test)]
mod validation_tests {
    use super::*;
    #[test]
    fn rejects_partial_wrong_root_and_invalid_list_values() {
        for body in [
            "",
            "<Error/>",
            "<ListBucketResult>",
            "<ListBucketResult><Contents><Key>x</Key></Contents>",
            "<ListBucketResult/><ListBucketResult/>",
            "<ListBucketResult/>garbage",
            "<ListBucketResult><Contents><Key>&unknown;</Key></Contents></ListBucketResult>",
            "<ListBucketResult><Contents><Size>-1</Size></Contents></ListBucketResult>",
            "<ListBucketResult><IsTruncated>maybe</IsTruncated></ListBucketResult>",
        ] {
            assert!(parse_list_xml(body).is_err(), "accepted {body}");
        }
        assert!(parse_list_xml("<?xml version=\"1.0\"?><ListBucketResult/>").is_ok());
        assert!(
            parse_initiate_mpu_xml("<InitiateMultipartUploadResult><UploadId>x</UploadId>")
                .is_err()
        );
    }
}

#[cfg(test)]
mod outgoing_text_tests {
    use super::*;

    fn xml_text_after_line_normalization(document: &str, element: &[u8]) -> String {
        // XML 1.0 section 2.11: normalize literal line endings before parsing
        // references. quick-xml's raw Text events do not do this for callers.
        let normalized = document.replace("\r\n", "\n").replace('\r', "\n");
        let mut reader = Reader::from_str(&normalized);
        reader.config_mut().trim_text(false);
        let mut inside = false;
        let mut text = String::new();
        loop {
            match reader.read_event().unwrap() {
                Event::Start(e) if e.name().as_ref() == element => inside = true,
                Event::End(e) if e.name().as_ref() == element => inside = false,
                Event::Text(e) if inside => text.push_str(&e.unescape().unwrap()),
                Event::Eof => break,
                _ => {}
            }
        }
        text
    }

    #[test]
    fn outgoing_delete_text_preserves_cr_lf_and_markup() {
        for key in ["a\rb", "a\r\nb", "a\nb", "文件\r<&>\t'\"", "literal&#13;"] {
            let xml = build_delete_xml(&[key]);
            assert_eq!(xml_text_after_line_normalization(&xml, b"Key"), key);
        }
    }

    #[test]
    fn outgoing_complete_text_preserves_cr_lf_and_markup() {
        for etag in ["a\rb", "a\r\nb", "a\nb", "opaque<&>", "literal&#13;"] {
            let xml = build_complete_mpu_xml(&[(1, etag)]);
            assert_eq!(xml_text_after_line_normalization(&xml, b"ETag"), format!("\"{etag}\""));
        }
    }
}
