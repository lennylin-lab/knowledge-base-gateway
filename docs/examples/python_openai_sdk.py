"""OpenAI SDK access through the gateway.

The SDK only needs base_url and the gateway API key; no provider secrets and
no provider URLs in client code.

    pip install openai

Verified against openai-python 3.5.0. Requires the gateway running with the
mock provider (see docs/developer-quickstart.md).
"""
import json
import uuid

from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:8080/v1",
    api_key="kb_dev_key_123",
    default_headers={"X-Request-ID": str(uuid.uuid4())},
)

# --- Chat Completions (non-streaming) -------------------------------------
chat = client.chat.completions.create(
    model="gateway-echo",
    messages=[{"role": "user", "content": "hello"}],
)
print("chat:", chat.choices[0].message.content, chat.usage.total_tokens)

# --- Chat Completions (streaming) -----------------------------------------
stream = client.chat.completions.create(
    model="gateway-echo",
    messages=[{"role": "user", "content": "hello"}],
    stream=True,
)
print("chat stream:", "".join(
    chunk.choices[0].delta.content or "" for chunk in stream))

# --- Responses (non-streaming) --------------------------------------------
response = client.responses.create(model="gateway-echo", input="hello")
print("responses:", response.status, response.output_text)

# --- Responses (streaming) -------------------------------------------------
# Typed events arrive in order and end with response.completed.
stream = client.responses.create(model="gateway-echo", input="hello", stream=True)
print("responses stream:", "".join(
    ev.delta for ev in stream if ev.type == "response.output_text.delta"))

# --- Responses (tool calling, manual loop) ---------------------------------
# The gateway transports tool calls; YOUR code executes tools and sends
# results back in a follow-up request. The gateway never runs tools.
#
# `tools` accepts the native flat Responses function shape and the nested
# chat-completions shape (`{"type": "function", "function": {...}}` via
# extra_body); both are documented in docs/api-versioning.md.
tools = [{
    "type": "function",
    "name": "get_weather",
    "description": "Look up current weather for a city",
    "parameters": {
        "type": "object",
        "properties": {"city": {"type": "string"}},
        "required": ["city"],
    },
}]
response = client.responses.create(
    model="gateway-echo", input="What is the weather in Paris?", tools=tools)
call = next(o for o in response.output if o.type == "function_call")
print("tool call:", call.name, call.arguments)

# (execute get_weather here, then continue the conversation)
follow_up = client.responses.create(
    model="gateway-echo",
    input=[
        {"type": "message", "role": "user", "content": "What is the weather in Paris?"},
        {"type": "function_call", "call_id": call.call_id, "name": call.name,
         "arguments": call.arguments},
        {"type": "function_call_output", "call_id": call.call_id,
         "output": json.dumps({"temp_c": 22, "sky": "sunny"})},
    ],
    tools=tools,
)
print("after tool:", follow_up.output_text)

# --- Responses (structured output) -----------------------------------------
# Structured output uses the SDK's native `text.format` parameter. The
# gateway MVP dialect (`response_format` with a nested json_schema object,
# passed through extra_body) is also accepted; the two are mutually
# exclusive.
schema = {
    "type": "object",
    "properties": {"echo": {"type": "string"}},
    "required": ["echo"],
    "additionalProperties": False,
}
response = client.responses.create(
    model="gateway-echo",
    input="hello",
    text={"format": {
        "type": "json_schema",
        "name": "echo_answer",
        "strict": True,
        "schema": schema,
    }},
)
print("structured:", json.loads(response.output_text))
