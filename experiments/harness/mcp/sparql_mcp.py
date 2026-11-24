#!/usr/bin/env python3
"""The database tool of an RDF cell as an MCP server: the twin of cypher_mcp.py.

    DBTOOL_URL=http://127.0.0.1:18080/cells/<cell>/tool python sparql_mcp.py [port]

One tool, `sparql(statement, passage, locator, id)`. SPARQL has no parameters, so the tool offers
none. Everything else is as in cypher_mcp.py: every call is forwarded unchanged to the database tool
(POST /cells/<cell>/tool), the tool's JSON answer goes back complete, as text, the caller's
`X-Tool-Key` header is passed on, and one process serves one cell. Streamable HTTP at /mcp,
stateless, JSON answers. Needs the `mcp` package in a version below 2.

A file of its own, so that cypher_mcp.py and the texts of its tool stay byte for byte as they are.

The tool description and the parameter descriptions below were written on 22 November 2026 as the
sentences of cypher_mcp.py with the language changed. They are text the model reads. Decided on
23 November 2026: they are part of the harness set-up and stay as written; the run record pins
them with the repository commit, beside the framework release. The model sees the tool as
`mcp__graph__<name>`, so the connector is named `graph` in every cell. A call whose arguments
have the wrong type is refused by this server and never reaches the database tool, so it leaves
no entry in the tool's record. As in cypher_mcp.py, the server announces the capability
"tools" and no other (no resources, no prompts).
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

from typing import Annotated

from mcp.server.fastmcp import Context, FastMCP
from pydantic import Field

DBTOOL_URL = os.environ.get("DBTOOL_URL", "http://127.0.0.1:18080/cells/dev-rdf/tool")
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 18081

DESCRIPTION = (
    "Run one SPARQL statement on the graph. A statement that reads returns its rows. "
    "A statement that writes is stored completely or not at all; give the passage of the "
    "source unit it rests on. The answer is JSON with a status: result, stored, "
    "already_present, rejected, error or refused."
)

server = FastMCP("graph", host="0.0.0.0", port=PORT, stateless_http=True, json_response=True)


def _only_tools(fastmcp: FastMCP) -> None:
    """Announce nothing but tools.

    FastMCP also registers handlers for resources and prompts, and the capabilities a server announces
    follow from its handlers. A framework that sees "resources" may offer the model tools of its own
    for them: SRW offered `read_mcp_resource` and `save_mcp_resource` in the pilot of 24 November 2026.
    The handlers are taken out here; if a version of the `mcp` package still announces more than tools
    afterwards, the wrapper does not start.
    """
    from mcp import types
    from mcp.server.lowlevel import NotificationOptions

    low = fastmcp._mcp_server
    for request in (types.ListResourcesRequest, types.ReadResourceRequest, types.ListResourceTemplatesRequest,
                    types.ListPromptsRequest, types.GetPromptRequest):
        low.request_handlers.pop(request, None)
    announced = low.get_capabilities(NotificationOptions(), {})
    if announced.resources is not None or announced.prompts is not None or announced.tools is None:
        raise SystemExit("the mcp package announces more than tools; the wrapper does not start")


_only_tools(server)


@server.tool(name="sparql", description=DESCRIPTION)
def sparql(
    statement: Annotated[str, Field(description="One SPARQL statement.")],
    ctx: Context,
    passage: Annotated[str | None, Field(description="The words of the source unit a write rests on, copied exactly.")] = None,
    locator: Annotated[str | None, Field(description="Where in the unit the passage stands, for example a section or a row.")] = None,
    id: Annotated[str | None, Field(description="The id an earlier answer gave for this write; only with a corrected statement.")] = None,
) -> str:
    body = {"statement": statement}
    for name, value in (("passage", passage), ("locator", locator), ("id", id)):
        if value is not None:
            body[name] = value
    headers = {"Content-Type": "application/json"}
    try:
        key = ctx.request_context.request.headers.get("x-tool-key")
    except Exception:
        key = None
    if key:
        headers["X-Tool-Key"] = key
    request = urllib.request.Request(
        DBTOOL_URL, data=json.dumps(body, ensure_ascii=False).encode("utf-8"), method="POST", headers=headers
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        return error.read().decode("utf-8") or json.dumps({"status": "error", "message": f"the database tool answered {error.code}"})
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        return json.dumps({"status": "error", "message": f"the database tool cannot be reached: {error}"})


if __name__ == "__main__":
    server.run(transport="streamable-http")
