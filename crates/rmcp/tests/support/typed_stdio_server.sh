#!/bin/sh

# Minimal line-delimited MCP server used to exercise TokioChildProcess without
# relying on a language-specific MCP implementation or package installation.
while IFS= read -r message; do
    request_id=$(printf '%s\n' "$message" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
    case "$message" in
        *'"method":"initialize"'*)
            printf '%s\n' "{\"jsonrpc\":\"2.0\",\"id\":${request_id},\"result\":{\"protocolVersion\":\"2025-03-26\",\"capabilities\":{},\"serverInfo\":{\"name\":\"typed-stdio-fixture\",\"version\":\"1.0.0\"}}}"
            ;;
        *'"method":"skills/list"'*)
            printf '%s\n' "{\"jsonrpc\":\"2.0\",\"id\":${request_id},\"result\":{\"resultType\":\"complete\",\"skills\":[\"stdio-example\"],\"_meta\":{\"vendorExtension\":{\"retained\":true}}}}"
            ;;
        *'"method":"ping"'*)
            printf '%s\n' "{\"jsonrpc\":\"2.0\",\"id\":${request_id},\"result\":{}}"
            ;;
        *'"method":"notifications/initialized"'*)
            ;;
    esac
done
