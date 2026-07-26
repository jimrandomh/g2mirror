//! JSON-with-comments support for config files: `//` line comments and
//! `/* */` block comments are erased before the text reaches serde_json.

/// Replace `//` and `/* */` comments (outside strings) with spaces.
///
/// Every comment byte becomes one space and newlines inside block comments
/// are kept, so the output has exactly the same length and line/column
/// layout as the input — serde_json error positions still point at the
/// right place in the commented file, and byte indices found in the
/// stripped text are valid in the original (which `--add-token`'s
/// comment-preserving insertion relies on). String contents, including
/// `\"` escapes and multi-byte characters, pass through untouched. An
/// unterminated block comment is stripped to end of input (serde_json then
/// reports the truncation).
pub fn strip_comments(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    let mut in_string = false;
    while i < bytes.len() {
        let b = bytes[i];
        if in_string {
            out.push(b);
            i += 1;
            match b {
                b'\\' if i < bytes.len() => {
                    out.push(bytes[i]);
                    i += 1;
                }
                b'"' => in_string = false,
                _ => {}
            }
        } else if b == b'/' && bytes.get(i + 1) == Some(&b'/') {
            while i < bytes.len() && bytes[i] != b'\n' {
                out.push(b' ');
                i += 1;
            }
        } else if b == b'/' && bytes.get(i + 1) == Some(&b'*') {
            out.extend_from_slice(b"  ");
            i += 2;
            while i < bytes.len() {
                if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                    out.extend_from_slice(b"  ");
                    i += 2;
                    break;
                }
                out.push(if bytes[i] == b'\n' { b'\n' } else { b' ' });
                i += 1;
            }
        } else {
            if b == b'"' {
                in_string = true;
            }
            out.push(b);
            i += 1;
        }
    }
    // Only ASCII bytes were substituted, so the result is valid UTF-8.
    String::from_utf8(out).expect("comment stripping preserved UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_and_block_comments_are_stripped() {
        let input = r#"{
  // a line comment
  "a": 1, // trailing
  /* block */ "b": /* mid */ 2,
  "c": /* multi
         line */ 3
}"#;
        let stripped = strip_comments(input);
        assert_eq!(stripped.len(), input.len(), "length must be preserved");
        assert_eq!(
            stripped.lines().count(),
            input.lines().count(),
            "line structure must be preserved"
        );
        let value: serde_json::Value = serde_json::from_str(&stripped).unwrap();
        assert_eq!(value, serde_json::json!({"a": 1, "b": 2, "c": 3}));
    }

    #[test]
    fn strings_are_untouched() {
        let input = r#"{"url": "http://x/*y*/z", "re": ".*//.*", "q": "a\"//b"}"#;
        assert_eq!(strip_comments(input), input);
        // A comment after a string containing escapes is still found.
        let input = r#"{"a": "\\" // c
}"#;
        let value: serde_json::Value =
            serde_json::from_str(&strip_comments(input)).unwrap();
        assert_eq!(value["a"], "\\");
    }

    #[test]
    fn multibyte_and_edge_cases() {
        // Multi-byte characters inside comments become one space per byte;
        // the result stays valid UTF-8 and parses.
        let input = "{\n  // héllo — comment\n  \"a\": \"é\"\n}";
        let stripped = strip_comments(input);
        assert_eq!(stripped.len(), input.len());
        let value: serde_json::Value = serde_json::from_str(&stripped).unwrap();
        assert_eq!(value["a"], "é");

        // A lone slash is not a comment.
        assert_eq!(strip_comments(r#"{"a": "b/c"}"#), r#"{"a": "b/c"}"#);

        // Unterminated block comment: stripped to the end.
        let stripped = strip_comments("{} /* dangling");
        assert_eq!(stripped.trim_end(), "{}");
        assert!(serde_json::from_str::<serde_json::Value>(&stripped).is_ok());
    }
}
