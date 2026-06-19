# Coder Mux Integration Support

Meridian includes dedicated, out-of-the-box support for **Coder Mux** workspaces. This integration ensures that the initial workspace configuration and initialization process operates smoothly.

## Background

When Coder Mux spins up a new workspace environment (such as a git worktree or a dedicated project directory), it sends a configuration/initialization request to the configured AI completion provider. During this initial turn, Coder Mux injects a specialized, custom tool block named `propose_name`. The purpose of this tool is to allow the AI model to suggest a clean, friendly name for the workspace folder path.

Because Meridian feeds OpenAI-compatible `/v1/chat/completions` request payloads directly into the rigid `@anthropic-ai/claude-code` SDK framework under the hood, the SDK strictly validates the tool schemas against what it is programmed to recognize. Since the SDK does not recognize `propose_name`, it drops/strips the tool block completely before feeding the request to the model. 

As a result, the model returns plain conversational text instead of executing a tool call, which immediately causes Coder Mux to error out with: `Model did not call propose_name tool`.

## Architecture & Solution

To resolve this issue gracefully and securely without introducing upstream hacks or complex schema overrides, Meridian employs a high-performance **path interceptor** at the entry of the OpenAI compatibility layer in `src/proxy/server.ts`.

### 1. Interceptor Trap (`src/proxy/server.ts`)

When a POST request lands on `/v1/chat/completions`, Meridian parses the payload and immediately checks if `propose_name` is present in the `tools` definition array.

```ts
if (isCoderMuxProposeNameRequest(rawBody)) {
  return c.json(handleCoderMuxProposeName(rawBody))
}
```

If detected, the request is caught by the interceptor and short-circuited. It never reaches the downstream SDK, preventing any translation failure or unexpected model behaviors on setup.

### 2. Name Generation and Truncation (`src/proxy/openai.ts`)

The pure function `handleCoderMuxProposeName` handles slug extraction and formatting:

1. **Extract Prompt**: It extracts the user message content. If it is a string, it uses it directly; if it is a structured array, it concatenates any text parts. If the user message is missing or empty, it falls back to a default folder name of `workspace`.
2. **Slug Formatting**: It cleanses the prompt of special characters, normalizes it to lowercase, trims whitespace, and replaces spaces with hyphens.
3. **Truncation (24 characters)**: It limits the workspace name to 24 characters as required by the environment folder layouts, falling back to `coder-mux-env` if the slug becomes empty after cleaning.
4. **Mocked Response Construction**: It crafts a fully OpenAI-compliant tool-call completion payload containing a mocked assistant message calling the `propose_name` tool with the generated folder name.

Because this hook only intercepts on the first turn when the `propose_name` tool is requested, subsequent turns do not contain this tool and fall back to standard, high-performance processing pipelines normally.

## Testing

Comprehensive tests are provided across the unit and integration layers to ensure complete correctness and prevent regressions.

### Unit Tests (`src/__tests__/openai.test.ts`)
* **`isCoderMuxProposeNameRequest`**: Verifies that the interceptor accurately detects the `propose_name` signature under different shapes.
* **`handleCoderMuxProposeName`**: Verifies correct slug normalization, special character removal, 24-character truncation, content array extraction, and safe defaults.

### Integration Tests (`src/__tests__/proxy-openai-compat.test.ts`)
* Runs HTTP-layer assertions against `/v1/chat/completions`.
* Binds the server to port `0` (using random ephemeral high ports), ensuring it does not conflict with any already-running `launchd` or `launchctl` Meridian instances on the host system.
