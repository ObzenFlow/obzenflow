use obzenflow_core::web::Request;
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub enum AuthConfig {
    /// No authentication (development only).
    None,
    /// API key in a header (e.g., `x-api-key`).
    ApiKey {
        header: String,
        keys: HashSet<String>,
    },
    /// Bearer token in the `Authorization` header.
    BearerToken { tokens: HashSet<String> },
    /// Webhook signature verification (HMAC-SHA256).
    WebhookSignature {
        secret: String,
        header: String,
        replay_window_secs: Option<u64>,
    },
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum AuthError {
    #[error("missing authentication header: {header}")]
    MissingHeader { header: String },
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error("invalid webhook signature header")]
    InvalidSignatureHeader,
    #[error("invalid webhook signature")]
    InvalidSignature,
    #[error("replay window exceeded")]
    ReplayAttack,
    #[error("invalid authorization header")]
    InvalidAuthorizationHeader,
}

pub fn authorize_request(auth: &AuthConfig, request: &Request) -> Result<(), AuthError> {
    match auth {
        AuthConfig::None => Ok(()),
        AuthConfig::ApiKey { header, keys } => {
            let provided = header_value(request, header)
                .ok_or_else(|| AuthError::MissingHeader { header: header.clone() })?;
            validate_api_key(provided, keys)
        }
        AuthConfig::BearerToken { tokens } => {
            let auth_header = header_value(request, "authorization")
                .ok_or_else(|| AuthError::MissingHeader {
                    header: "Authorization".to_string(),
                })?;
            let token = parse_bearer_token(auth_header)?;
            validate_bearer_token(token, tokens)
        }
        AuthConfig::WebhookSignature {
            secret,
            header,
            replay_window_secs,
        } => {
            let signature_header = header_value(request, header)
                .ok_or_else(|| AuthError::MissingHeader { header: header.clone() })?;
            validate_webhook_signature(
                &request.body,
                signature_header,
                secret,
                replay_window_secs,
            )
        }
    }
}

fn header_value<'a>(request: &'a Request, header_name: &str) -> Option<&'a str> {
    request.headers.iter().find_map(|(k, v)| {
        if k.eq_ignore_ascii_case(header_name) {
            Some(v.as_str())
        } else {
            None
        }
    })
}

fn validate_api_key(provided: &str, valid_keys: &HashSet<String>) -> Result<(), AuthError> {
    use subtle::ConstantTimeEq;

    let provided = provided.as_bytes();
    let ok = valid_keys
        .iter()
        .any(|key| provided.ct_eq(key.as_bytes()).into());
    if ok {
        Ok(())
    } else {
        Err(AuthError::InvalidCredentials)
    }
}

fn parse_bearer_token(auth_header: &str) -> Result<&str, AuthError> {
    let mut parts = auth_header.splitn(2, ' ');
    let scheme = parts.next().unwrap_or_default();
    let token = parts.next().unwrap_or_default();

    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() {
        return Err(AuthError::InvalidAuthorizationHeader);
    }

    Ok(token)
}

fn validate_bearer_token(provided: &str, valid_tokens: &HashSet<String>) -> Result<(), AuthError> {
    use subtle::ConstantTimeEq;

    let provided = provided.as_bytes();
    let ok = valid_tokens
        .iter()
        .any(|token| provided.ct_eq(token.as_bytes()).into());
    if ok {
        Ok(())
    } else {
        Err(AuthError::InvalidCredentials)
    }
}

fn validate_webhook_signature(
    raw_body: &[u8],
    signature_header: &str,
    secret: &str,
    replay_window_secs: &Option<u64>,
) -> Result<(), AuthError> {
    use ring::hmac;
    use subtle::ConstantTimeEq;

    let parsed = parse_signature_header(signature_header)?;

    if let (Some(ts), Some(window)) = (parsed.timestamp, *replay_window_secs) {
        let now = chrono::Utc::now().timestamp();
        if (now - ts).abs() > window as i64 {
            return Err(AuthError::ReplayAttack);
        }
    }

    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    let expected = if let Some(ts_str) = parsed.timestamp_string.as_deref() {
        let mut signed =
            Vec::with_capacity(ts_str.as_bytes().len().saturating_add(1).saturating_add(raw_body.len()));
        signed.extend_from_slice(ts_str.as_bytes());
        signed.push(b'.');
        signed.extend_from_slice(raw_body);
        hmac::sign(&key, &signed)
    } else {
        hmac::sign(&key, raw_body)
    };

    let expected = expected.as_ref();
    let provided = decode_hex(&parsed.signature_hex)?;
    if expected.ct_eq(provided.as_slice()).into() {
        Ok(())
    } else {
        Err(AuthError::InvalidSignature)
    }
}

#[derive(Debug)]
struct ParsedSignatureHeader {
    signature_hex: String,
    timestamp: Option<i64>,
    timestamp_string: Option<String>,
}

fn parse_signature_header(value: &str) -> Result<ParsedSignatureHeader, AuthError> {
    // Stripe-style: "t=123,v1=abcdef..."
    if value.contains("t=") && value.contains("v1=") {
        let mut timestamp: Option<i64> = None;
        let mut timestamp_string: Option<String> = None;
        let mut signature_hex: Option<String> = None;
        for part in value.split(',') {
            let part = part.trim();
            if let Some(v) = part.strip_prefix("t=") {
                let v = v.trim();
                timestamp_string = Some(v.to_string());
                timestamp = v.parse::<i64>().ok();
            } else if let Some(v) = part.strip_prefix("v1=") {
                signature_hex = Some(v.trim().to_string());
            }
        }

        let signature_hex = signature_hex.ok_or(AuthError::InvalidSignatureHeader)?;
        return Ok(ParsedSignatureHeader {
            signature_hex,
            timestamp,
            timestamp_string,
        });
    }

    // GitHub-style: "sha256=<hex>"
    if let Some(hex) = value.strip_prefix("sha256=") {
        return Ok(ParsedSignatureHeader {
            signature_hex: hex.trim().to_string(),
            timestamp: None,
            timestamp_string: None,
        });
    }

    // Fallback: treat value as raw hex.
    Ok(ParsedSignatureHeader {
        signature_hex: value.trim().to_string(),
        timestamp: None,
        timestamp_string: None,
    })
}

fn decode_hex(value: &str) -> Result<Vec<u8>, AuthError> {
    let value = value.trim();
    if value.len() % 2 != 0 {
        return Err(AuthError::InvalidSignatureHeader);
    }

    let mut out = Vec::with_capacity(value.len() / 2);
    let mut it = value.as_bytes().iter().copied();
    while let (Some(hi), Some(lo)) = (it.next(), it.next()) {
        let hi = from_hex_digit(hi)?;
        let lo = from_hex_digit(lo)?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn from_hex_digit(b: u8) -> Result<u8, AuthError> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(AuthError::InvalidSignatureHeader),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::web::HttpMethod;

    #[test]
    fn api_key_authorizes_with_constant_time_compare() {
        let mut keys = HashSet::new();
        keys.insert("secret".to_string());
        let auth = AuthConfig::ApiKey {
            header: "x-api-key".to_string(),
            keys,
        };

        let request = Request::new(HttpMethod::Post, "/".to_string())
            .with_header("x-api-key".to_string(), "secret".to_string());
        authorize_request(&auth, &request).unwrap();

        let request = Request::new(HttpMethod::Post, "/".to_string())
            .with_header("x-api-key".to_string(), "wrong".to_string());
        assert!(authorize_request(&auth, &request).is_err());
    }

    #[test]
    fn bearer_authorizes_with_authorization_header() {
        let mut tokens = HashSet::new();
        tokens.insert("token123".to_string());
        let auth = AuthConfig::BearerToken { tokens };

        let request = Request::new(HttpMethod::Post, "/".to_string()).with_header(
            "Authorization".to_string(),
            "Bearer token123".to_string(),
        );
        authorize_request(&auth, &request).unwrap();
    }

    #[test]
    fn webhook_signature_validates_raw_body_hmac() {
        let auth = AuthConfig::WebhookSignature {
            secret: "secret".to_string(),
            header: "x-sig".to_string(),
            replay_window_secs: None,
        };

        // For the non-timestamp format, signature header is raw hex of HMAC(secret, body).
        use ring::hmac;
        let body = br#"{"a":1}"#;
        let key = hmac::Key::new(hmac::HMAC_SHA256, b"secret");
        let sig = hmac::sign(&key, body);
        let sig_hex = sig.as_ref().iter().map(|b| format!("{:02x}", b)).collect::<String>();

        let request = Request::new(HttpMethod::Post, "/".to_string())
            .with_header("x-sig".to_string(), sig_hex)
            .with_body(body.to_vec());
        authorize_request(&auth, &request).unwrap();
    }
}

