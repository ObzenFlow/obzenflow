pub fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key).ok().and_then(|v| v.parse().ok())
}

pub fn env_bool(key: &str) -> Option<bool> {
    let raw = std::env::var(key).ok()?;
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        "0" | "false" | "no" | "n" | "off" => Some(false),
        _ => None,
    }
}

pub fn truncate_chars(s: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }

    let mut iter = s.chars();
    for _ in 0..max_chars {
        if iter.next().is_none() {
            return s.to_string();
        }
    }

    if iter.next().is_none() {
        return s.to_string();
    }

    if max_chars <= 3 {
        return "...".to_string();
    }

    let mut out = s.chars().take(max_chars - 3).collect::<String>();
    out.push_str("...");
    out
}

