#![cfg(feature = "server")]

use rmcp::model::ServerCapabilities;
use serde_json::json;

#[test]
fn events_capability_is_opt_in_and_preserves_experimental_capabilities() {
    assert!(
        serde_json::to_value(ServerCapabilities::default())
            .unwrap()
            .get("events")
            .is_none()
    );

    let mut capabilities = ServerCapabilities::builder()
        .enable_events()
        .enable_tools()
        .enable_tool_list_changed()
        .enable_resources()
        .enable_resources_list_changed()
        .enable_prompts()
        .enable_prompts_list_changed()
        .build();
    capabilities.experimental =
        Some(serde_json::from_value(json!({"claude/channel": {}})).unwrap());
    let encoded = serde_json::to_value(&capabilities).unwrap();
    assert_eq!(encoded["events"], json!({}));
    assert_eq!(encoded["experimental"]["claude/channel"], json!({}));
    assert_eq!(encoded["tools"]["listChanged"], true);
    assert_eq!(encoded["resources"]["listChanged"], true);
    assert_eq!(encoded["prompts"]["listChanged"], true);
    assert_eq!(
        serde_json::from_value::<ServerCapabilities>(encoded).unwrap(),
        capabilities
    );
}

#[test]
fn events_capability_settings_round_trip() {
    let capabilities: ServerCapabilities =
        serde_json::from_value(json!({"events":{"listChanged":true}})).unwrap();
    assert_eq!(
        serde_json::to_value(capabilities).unwrap()["events"],
        json!({"listChanged":true})
    );
}
