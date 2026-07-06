//! Prometheus text-format rendering of the daemon status document.
//!
//! Renders from the same JSON the control socket serves, so the two admin
//! surfaces can never disagree. Hand-rolled: the exposition format is a few
//! lines per metric and not worth a dependency.

use std::fmt::Write;

use serde_json::Value;

pub(crate) fn render_prometheus(status: &Value) -> String {
    let mut out = String::new();

    gauge(&mut out, "whdr_uptime_ms", &[], &status["uptime_ms"]);

    if let Some(extensions) = status["extensions"].as_array() {
        for ext in extensions {
            let Some(id) = ext["id"].as_str() else {
                continue;
            };
            let labels = [("ext", id)];
            let up = if ext["state"].as_str() == Some("Ready") {
                Value::from(1)
            } else {
                Value::from(0)
            };
            gauge(&mut out, "whdr_ext_up", &labels, &up);
            for (metric, field) in [
                ("whdr_ext_restarts", "restarts"),
                ("whdr_ext_in_flight", "in_flight"),
                ("whdr_ext_protocol_errors", "protocol_errors"),
                ("whdr_ext_namespace_violations", "namespace_violations"),
                ("whdr_ext_consecutive_timeouts", "consecutive_timeouts"),
                ("whdr_ext_events_emitted", "events_emitted"),
                ("whdr_ext_last_event_at_ms", "last_event_at_ms"),
            ] {
                if !ext[field].is_null() {
                    gauge(&mut out, metric, &labels, &ext[field]);
                }
            }
        }
    }

    if let Some(subscribers) = status["subscribers"].as_array() {
        // Multiple connections may share a name (same token); sum them so
        // series stay stable across reconnects.
        let mut by_name: Vec<(&str, u64, u64, u64)> = Vec::new();
        for subscriber in subscribers {
            let Some(name) = subscriber["name"].as_str() else {
                continue;
            };
            let delivered = subscriber["delivered"].as_u64().unwrap_or(0);
            let dropped = subscriber["dropped"].as_u64().unwrap_or(0);
            match by_name.iter_mut().find(|(n, ..)| *n == name) {
                Some(row) => {
                    row.1 += delivered;
                    row.2 += dropped;
                    row.3 += 1;
                }
                None => by_name.push((name, delivered, dropped, 1)),
            }
        }
        for (name, delivered, dropped, connections) in by_name {
            let labels = [("name", name)];
            gauge(
                &mut out,
                "whdr_subscriber_delivered",
                &labels,
                &Value::from(delivered),
            );
            gauge(
                &mut out,
                "whdr_subscriber_dropped",
                &labels,
                &Value::from(dropped),
            );
            gauge(
                &mut out,
                "whdr_subscriber_connections",
                &labels,
                &Value::from(connections),
            );
        }
    }

    let global = &status["global"];
    for (metric, field) in [
        ("whdr_routes", "routes"),
        ("whdr_unavailable_routes", "unavailable_routes"),
        ("whdr_channel_prefixes", "channel_prefixes"),
        ("whdr_subscriber_count", "subscriber_count"),
    ] {
        gauge(&mut out, metric, &[], &global[field]);
    }

    out
}

fn gauge(out: &mut String, name: &str, labels: &[(&str, &str)], value: &Value) {
    let rendered = match value {
        Value::Number(number) => number.to_string(),
        Value::Null => return,
        other => match other.as_str() {
            Some(text) => text.to_string(),
            None => return,
        },
    };
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (index, (key, label_value)) in labels.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            let _ = write!(out, "{key}=\"{}\"", escape_label(label_value));
        }
        out.push('}');
    }
    let _ = writeln!(out, " {rendered}");
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn renders_extension_subscriber_and_global_series() {
        let status = json!({
            "uptime_ms": 1234,
            "extensions": [
                {
                    "id": "github",
                    "state": "Ready",
                    "restarts": 2,
                    "in_flight": 1,
                    "protocol_errors": 0,
                    "namespace_violations": 0,
                    "consecutive_timeouts": 0,
                    "events_emitted": 42,
                    "last_event_at_ms": 1751760000000u64,
                },
                { "id": "teams", "state": "Failed", "reason": "crashloop" },
            ],
            "subscribers": [
                { "name": "project-a", "delivered": 10, "dropped": 1 },
                { "name": "project-a", "delivered": 5, "dropped": 0 },
            ],
            "global": {
                "routes": 3,
                "unavailable_routes": 0,
                "channel_prefixes": 2,
                "subscriber_count": 2,
            },
        });

        let text = render_prometheus(&status);

        assert!(text.contains("whdr_uptime_ms 1234\n"));
        assert!(text.contains("whdr_ext_up{ext=\"github\"} 1\n"));
        assert!(text.contains("whdr_ext_up{ext=\"teams\"} 0\n"));
        assert!(text.contains("whdr_ext_events_emitted{ext=\"github\"} 42\n"));
        assert!(text.contains("whdr_ext_last_event_at_ms{ext=\"github\"} 1751760000000\n"));
        // Failed ext rows carry no counters; only the up gauge is emitted.
        assert!(!text.contains("whdr_ext_restarts{ext=\"teams\"}"));
        // Same-name connections are summed.
        assert!(text.contains("whdr_subscriber_delivered{name=\"project-a\"} 15\n"));
        assert!(text.contains("whdr_subscriber_dropped{name=\"project-a\"} 1\n"));
        assert!(text.contains("whdr_subscriber_connections{name=\"project-a\"} 2\n"));
        assert!(text.contains("whdr_subscriber_count 2\n"));
    }

    #[test]
    fn label_values_are_escaped() {
        let status = json!({
            "uptime_ms": 1,
            "extensions": [],
            "subscribers": [
                { "name": "we\"ird\\name", "delivered": 1, "dropped": 0 },
            ],
            "global": {},
        });

        let text = render_prometheus(&status);
        assert!(text.contains(r#"whdr_subscriber_delivered{name="we\"ird\\name"} 1"#));
    }
}
