"""Define a custom Reasoning and Action agent.

Works with a chat model with tool calling support.
"""

import json
from datetime import UTC, datetime
from typing import Literal, cast

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.graph import END, StateGraph
from langgraph.prebuilt import ToolNode
from langgraph.runtime import Runtime
from langgraph.types import Command, interrupt

from react_agent_hitl.context import Context
from react_agent_hitl.state import InputState, State
from react_agent_hitl.tools import TOOLS
from react_agent_hitl.utils import load_chat_model

# Define the function that calls the model


async def call_model(state: State, runtime: Runtime[Context]) -> dict[str, list[AIMessage]]:
    """Call the LLM powering our "agent".

    This function prepares the prompt, initializes the model, and processes the response.

    Args:
        state (State): The current state of the conversation.
        config (RunnableConfig): Configuration for the model run.

    Returns:
        dict: A dictionary containing the model's response message.
    """
    # Initialize the model with tool binding. Change the model or add more tools here.
    model = load_chat_model(runtime.context.model).bind_tools(TOOLS)

    # Format the system prompt. Customize this to change the agent's behavior.
    system_message = runtime.context.system_prompt.format(system_time=datetime.now(tz=UTC).isoformat())

    # Get the model's response
    response = cast(
        "AIMessage",
        await model.ainvoke([{"role": "system", "content": system_message}, *state.messages]),
    )

    # Handle the case when it's the last step and the model still wants to use a tool
    if state.is_last_step and response.tool_calls:
        return {
            "messages": [
                AIMessage(
                    id=response.id,
                    content="Sorry, I could not find an answer to your question in the specified number of steps.",
                )
            ]
        }

    # Return the model's response as a list to be added to existing messages
    return {"messages": [response]}


def _find_tool_message(messages: list) -> AIMessage | None:
    """Find the last AI message with tool calls."""
    for msg in reversed(messages):
        if isinstance(msg, AIMessage) and hasattr(msg, "tool_calls") and msg.tool_calls:
            return msg
    return None


def _create_tool_cancellations(tool_calls: list, reason: str) -> list[ToolMessage]:
    """Create cancellation messages for tool calls."""
    return [
        ToolMessage(content=f"Tool execution {reason}.", tool_call_id=tc["id"], name=tc["name"]) for tc in tool_calls
    ]


def _parse_args(args) -> dict:
    """Parse args, handling JSON strings."""
    if isinstance(args, str):
        try:
            return json.loads(args)
        except json.JSONDecodeError:
            return {}
    return args if isinstance(args, dict) else {}


def _update_tool_calls(original_calls: list, edited_args: dict) -> list:
    """Update tool calls with edited arguments."""
    updated_calls = []
    for call in original_calls:
        updated_call = call.copy()
        tool_name = call["name"]

        if tool_name in edited_args.get("args", {}):
            updated_call["args"] = _parse_args(edited_args["args"][tool_name])
        else:
            updated_call["args"] = _parse_args(call["args"])

        updated_calls.append(updated_call)
    return updated_calls


async def human_approval(state: State) -> Command:
    """Request human approval before executing tools."""
    # TODO: Fix Mark as Resolved functionality
    # ISSUE: Command(goto=END) creates infinite loop due to LangGraph bug
    # GITHUB ISSUE: https://github.com/langchain-ai/langgraph/issues/5572
    # The goto=END command gets ignored and creates "branch:to:__end__" channel error
    tool_message = _find_tool_message(state.messages)
    if not tool_message:
        return Command(goto=END)

    human_response = interrupt(
        {
            "action_requests": [
                {
                    "name": tc["name"],
                    "args": tc.get("args", {}),
                    "description": f"Approve `{tc['name']}` before it runs.",
                }
                for tc in tool_message.tool_calls
            ],
            "review_configs": [
                {
                    "action_name": tc["name"],
                    "allowed_decisions": ["approve", "edit", "reject", "respond"],
                }
                for tc in tool_message.tool_calls
            ],
        }
    )

    decisions = human_response.get("decisions") if isinstance(human_response, dict) else None
    if not decisions or not isinstance(decisions, list):
        return Command(goto=END)

    decision = decisions[0]
    decision_type = decision.get("type", "")
    message = decision.get("message")

    if decision_type == "approve":
        return Command(goto="tools")

    if decision_type == "respond":
        tool_responses = _create_tool_cancellations(tool_message.tool_calls, "was interrupted for human input")
        human_message = HumanMessage(content=str(message))
        return Command(goto="call_model", update={"messages": tool_responses + [human_message]})

    edited = decision.get("edited_action")
    if decision_type == "edit" and isinstance(edited, dict) and "args" in edited:
        updated_calls = _update_tool_calls(tool_message.tool_calls, edited)
        updated_message = AIMessage(content=tool_message.content, tool_calls=updated_calls, id=tool_message.id)
        return Command(goto="tools", update={"messages": [updated_message]})

    reason = str(message or "cancelled by human operator") if decision_type == "reject" else "invalid format"
    tool_responses = _create_tool_cancellations(tool_message.tool_calls, reason)
    return Command(goto=END, update={"messages": tool_responses})


# Define a new graph

builder = StateGraph(State, input_schema=InputState, context_schema=Context)

# Define the nodes we will cycle between
builder.add_node(call_model)
builder.add_node("tools", ToolNode(TOOLS))
builder.add_node(human_approval)

# Set the entrypoint as `call_model`
# This means that this node is the first one called
builder.add_edge("__start__", "call_model")


def route_model_output(state: State) -> Literal["__end__", "human_approval"]:
    """Determine the next node based on the model's output.

    This function checks if the model's last message contains tool calls.
    If it does, we route to human approval first.

    Args:
        state (State): The current state of the conversation.

    Returns:
        str: The name of the next node to call ("__end__" or "human_approval").
    """
    last_message = state.messages[-1]
    if not isinstance(last_message, AIMessage):
        raise ValueError(f"Expected AIMessage in output edges, but got {type(last_message).__name__}")
    # If there is no tool call, then we finish
    if not last_message.tool_calls:
        return "__end__"
    # Otherwise we need human approval first
    return "human_approval"


# Add conditional edges
builder.add_conditional_edges("call_model", route_model_output, path_map=["human_approval", END])


# Add a normal edge from `tools` to `call_model`
# This creates a cycle: after using tools, we always return to the model
builder.add_edge("tools", "call_model")

# Compile the builder into an executable graph
graph = builder.compile(name="ReAct Agent")
