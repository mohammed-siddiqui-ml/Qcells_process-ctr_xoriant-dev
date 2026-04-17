# CTR Processing Lambda Function (merged)
# --------------------------------------
# - Reads a call summary from DynamoDB (ConnectCallTimeline) by RootContactId
# - Builds a human-readable timeline (sorted ascending)
# - Posts the timeline as a Salesforce CaseComment using shared auth
#
# Event (recommended):
#   { "rootContactId": "<UUID>" }
#
# Env Vars:
#   TIMELINE_TABLE=ConnectCallTimeline
#   OUTPUT_TZ=Asia/Kolkata   (optional, default)
#   SHOW_UTC=false           (optional; 'true' appends UTC time)
#
# Requires:
#   from common.sf_auth import get_access_token

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Tuple
from zoneinfo import ZoneInfo

import boto3
from boto3.dynamodb.types import TypeDeserializer
import requests

# ---- Shared Salesforce auth (your existing utility) ----
from common.sf_auth import get_access_token

# ---- AWS resources ----
DDB = boto3.resource("dynamodb")
TABLE = DDB.Table(os.environ.get("TIMELINE_TABLE", "ConnectCallTimeline"))

SALESFORCE_TASK_BASE_URL = os.environ.get(
    "SALES_FORCE_TASK_BASE_URL",
    "https://qcellsnorthamerica123--qa.sandbox.lightning.force.com/lightning/r/Task/"
)
SALESFORCE_S3_PRESIGNED_BASE_URL = os.environ.get(
    "SALESFORCE_S3_PRESIGNED_URL",
    "https://qcellsnorthamerica123--qa.sandbox.my.salesforce-setup.com/apex/S3Redirect?"
)

_deser = TypeDeserializer()

# -----------------------------
# Utilities
# -----------------------------

def iso_to_dt(s: Optional[str]) -> Optional[datetime]:
    if not s or not isinstance(s, str):
        return None
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None

def dt_to_iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def is_attrvalue_item(item: Dict[str, Any]) -> bool:
    for v in item.values():
        if isinstance(v, dict) and any(k in v for k in ("S","N","M","L","BOOL","NULL","B","SS","NS","BS")):
            return True
    return False

def to_native(item: Dict[str, Any]) -> Dict[str, Any]:
    if not item or not is_attrvalue_item(item):
        return item
    return { k: _deser.deserialize(v) for k, v in item.items() }

def safe_get(d: Dict[str, Any], *path, default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur

def fmt_time_local(dt: datetime, tz: ZoneInfo, show_utc: bool) -> str:
    if not isinstance(dt, datetime):
        return ""
    lt = dt.astimezone(tz)
    s_local = lt.strftime("%H:%M:%S")
    if show_utc:
        s_utc = dt.astimezone(timezone.utc).strftime("%H:%M:%SZ")
        return f"{s_local} ({s_utc})"
    return s_local

# Event priority for tie-breaking (same-second events)
_EVENT_ORDER = {
    "CALL_STARTED"      : 10,
    "AGENT_JOINED"      : 20,
    "TRANSFER_COMPLETED": 30,
    "AGENT_LEFT"        : 40,
    "CALL_ENDED"        : 50,
}

# -----------------------------
# Timeline builder (from the ConnectCallTimeline item)
# -----------------------------

def build_timeline_payload(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build both structured timeline AND human-readable text block in the format you asked:

    Call Summary:

    18:08:40 Customer connected the call.
    18:09:34 Agent Mohammed-Siddiqui answered.
    18:09:54 Agent Mohammed-Siddiqui transferred the call to Agent aman.singh
    18:10:14 Agent aman.singh joined the call
    18:11:00 Agent Mohammed-Siddiqui left the call
    18:11:44 Agent aman.singh left the call
    18:11:53 Primary call ended and was disconnect by CUSTOMER_DISCONNECT

    Recording Links:
    - <url>
    """

    # Output formatting config

    tz_name = os.environ.get("OUTPUT_TZ", "Asia/Kolkata")
    show_utc = os.environ.get("SHOW_UTC", "false").lower() in ("1", "true", "yes")
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")

    root_id: str    = item.get("RootContactId")
    call_started    = item.get("CallStartedTs")
    call_ended      = item.get("CallEndedTs")

    legs = item.get("ContactLegs", {}) or {}
    agents = item.get("Agents", {}) or {}

    #Collect CaseIds from agents
    case_ids: List[str] = []
    for _, agent in agents.items():
        cid = agent.get("CaseId")
        if cid and cid not in case_ids:
            case_ids.append(cid)

    root_leg = legs.get(root_id, {}) or {}
    customer_endpoint = root_leg.get("CustomerEndpoint")
    initial_queue = root_leg.get("QueueName")
    root_disconnect_reason = root_leg.get("DisconnectReason")
    inbound_agent = root_leg.get("AgentUsername")
    inbound_transfer_ts = iso_to_dt(root_leg.get("TransferCompletedTimestamp"))

    # Earliest agent to join (for “answered” phrasing)
    first_agent, first_join_dt = None, None
    for a, info in agents.items():
        fj = iso_to_dt(info.get("firstJoin"))
        if fj and (first_join_dt is None or fj < first_join_dt):
            first_agent, first_join_dt = a, fj

    # Transfer legs and chain
    transfer_legs = [(cid, l) for cid, l in legs.items() if l.get("InitiationMethod") == "TRANSFER"]

    # Join chain (inbound → each transfer agent)
    chain: List[Tuple[str, datetime]] = []
    if inbound_agent:
        ia_first = iso_to_dt(safe_get(agents, inbound_agent, "firstJoin")) or iso_to_dt(call_started)
        if ia_first:
            chain.append((inbound_agent, ia_first))

    for _, lg in transfer_legs:
        dst = lg.get("AgentUsername")
        if not dst:
            continue
        jdt = iso_to_dt(safe_get(agents, dst, "firstJoin"))
        if jdt:
            chain.append((dst, jdt))

    chain.sort(key=lambda x: x[1])

    transfer_chain = []
    for i in range(1, len(chain)):
        src = chain[i-1][0]
        dst = chain[i][0]
        when = inbound_transfer_ts if (i == 1 and inbound_transfer_ts) else chain[i][1]
        transfer_chain.append({"from": src, "to": dst, "at": dt_to_iso(when)})

    events = []

    if call_started:
        events.append({"type": "CALL_STARTED", "ts": call_started})

    for a, info in agents.items():
        if info.get("firstJoin"):
            events.append({"type": "AGENT_JOINED", "ts": info["firstJoin"], "agent": a})

    for t in transfer_chain:
        events.append({"type": "TRANSFER_COMPLETED", "ts": t["at"], "from": t["from"], "to": t["to"]})

    for a, info in agents.items():
        if info.get("lastLeave"):
            events.append({"type": "AGENT_LEFT", "ts": info["lastLeave"], "agent": a})

    if call_ended:
        events.append({"type": "CALL_ENDED", "ts": call_ended, "reason": root_disconnect_reason})

    events.sort(key=lambda ev: (
        iso_to_dt(ev.get("ts")) or datetime.max.replace(tzinfo=timezone.utc),
        _EVENT_ORDER.get(ev.get("type"), 999)
    ))

    text_lines = ["Call Summary:\n"]

    def render_line(ev):
        stamp = dt_to_iso(iso_to_dt(ev.get("ts")))
        if ev["type"] == "CALL_STARTED":
            return f"{stamp} Customer connected the call."
        if ev["type"] == "AGENT_JOINED":
            if ev["agent"] == first_agent:
                return f"{stamp} Agent {ev['agent']} answered."
            return f"{stamp} Agent {ev['agent']} joined the call"
        if ev["type"] == "TRANSFER_COMPLETED":
            return f"{stamp} Agent {ev['from']} transferred the call to Agent {ev['to']}"
        if ev["type"] == "AGENT_LEFT":
            return f"{stamp} Agent {ev['agent']} left the call"
        if ev["type"] == "CALL_ENDED":
            return f"{stamp} Primary call ended and was disconnect by {ev.get('reason', 'UNKNOWN')}"
        return None

    for ev in events:
        line = render_line(ev)
        if line:
            text_lines.append(line)

    text_lines.append("\n\nTask Forms Summary:\n")
    for name, agent in agents.items():
        if agent.get("TaskFormId"):
            text_lines.append(
                f" Task form filled by Agent, {name}: {SALESFORCE_TASK_BASE_URL}{agent['TaskFormId']}/view"
            )
        else:
            text_lines.append(f" Task form not filled by Agent, {name}")

    return {
        "meta": {
            "rootContactId": root_id,
            "caseIds": case_ids,
            "customerEndpoint": customer_endpoint,
            "initialQueue": initial_queue
        },
        "timelineTextBlock": "\n".join(text_lines)
    }

# -----------------------------
# Salesforce API
# -----------------------------

def post_case_comment(case_id: str, comment_body: str):
    token = get_access_token()
    url = f"{token['instance_url']}/services/data/v59.0/sobjects/CaseComment"
    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token['access_token']}",
            "Content-Type": "application/json"
        },
        json={"ParentId": case_id, "CommentBody": comment_body, "IsPublished": False},
        timeout=20,
    )
    if resp.status_code not in (200, 201):
        raise RuntimeError(resp.text)
    return resp.json()

# -----------------------------
# Lambda handler
# -----------------------------

def lambda_handler(event, context):

    root_id = event.get("rootContactId") or event.get("RootContactId")
    if not root_id:
        return {"statusCode": 400, "body": "Missing rootContactId"}

    resp = TABLE.get_item(Key={"RootContactId": root_id}, ConsistentRead=True)
    if "Item" not in resp:
        return {"statusCode": 404, "body": "Item not found"}

    result = build_timeline_payload(to_native(resp["Item"]))

    posted = []
    failed = []

    for case_id in result["meta"]["caseIds"]:
        try:
            post_case_comment(case_id, result["timelineTextBlock"])
            posted.append(case_id)
        except Exception as e:
            failed.append({"caseId": case_id, "error": str(e)})

    return {
        "statusCode": 200 if posted else 207,
        "body": json.dumps({
            "rootContactId": root_id,
            "postedCases": posted,
            "failedCases": failed
        })
    }