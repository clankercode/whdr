use whdr_server::route_key_from_path;

#[test]
fn route_key_uses_first_non_empty_path_segment() {
    assert_eq!(
        route_key_from_path("/github/hooks"),
        Some("github".to_string())
    );
    assert_eq!(route_key_from_path("teams"), Some("teams".to_string()));
    assert_eq!(route_key_from_path("/"), None);
    assert_eq!(route_key_from_path(""), None);
}
