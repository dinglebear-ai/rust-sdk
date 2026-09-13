#![cfg(all(
    unix,
    not(feature = "local"),
    feature = "transport-child-process",
    feature = "client"
))]

use rmcp::{
    ServiceExt,
    model::{ClientRequest, CustomRequest, PingRequest, ServerResult},
    transport::{ConfigureCommandExt, TokioChildProcess},
};
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct SkillsListResult {
    result_type: String,
    skills: Vec<String>,
    #[serde(rename = "_meta")]
    meta: serde_json::Value,
}

#[tokio::test]
async fn typed_extension_response_survives_child_process_and_preserves_correlation()
-> anyhow::Result<()> {
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/support/typed_stdio_server.sh"
    );
    let transport =
        TokioChildProcess::new(tokio::process::Command::new("sh").configure(|command| {
            command.arg(fixture);
        }))?;
    let client = ().serve(transport).await?;

    let response: SkillsListResult = client
        .send_request_as(ClientRequest::CustomRequest(CustomRequest::new(
            "skills/list",
            None,
        )))
        .await?;
    assert_eq!(response.result_type, "complete");
    assert_eq!(response.skills, ["stdio-example"]);
    assert_eq!(response.meta["vendorExtension"]["retained"], true);

    let following = client
        .send_request(ClientRequest::PingRequest(PingRequest::default()))
        .await?;
    assert!(matches!(following, ServerResult::EmptyResult(_)));

    client.cancel().await?;
    Ok(())
}
