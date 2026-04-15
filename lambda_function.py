
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
SALESFORCE_TASK_BASE_URL = os.environ.get("SALES_FORCE_TASK_BASE_URL", "https://qcellsnorthamerica123--qa.sandbox.lightning.force.com/lightning/r/Task/")
SALESFORCE_S3_PRESIGNED_BASE_URL = os.environ.get("SALESFORCE_S3_PRESIGNED_URL", "https://qcellsnorthamerica123--qa.sandbox.my.salesforce-setup.com/apex/S3Redirect?")

_deser = TypeDeserializer()

# -----------------------------
# Utilities
# -----------------------------

def iso_to_dt(s: Optional[str]) -> Optional[datetime]:
    """Parse ISO-8601 Z timestamps into aware UTC datetime."""
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
    """Render 'HH:MM:SS' in local tz; optionally append ' (HH:MM:SSZ)'."""
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
    tz_name  = os.environ.get("OUTPUT_TZ", "Asia/Kolkata")
    show_utc = os.environ.get("SHOW_UTC", "false").lower() in ("1", "true", "yes")
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")

    # Core fields
    root_id: str                 = item.get("RootContactId")
    case_id: Optional[str]       = item.get("CaseId")
    call_started: Optional[str]  = item.get("CallStartedTs")
    call_ended: Optional[str]    = item.get("CallEndedTs")

    legs: Dict[str, Dict[str, Any]]   = item.get("ContactLegs", {}) or {}
    agents: Dict[str, Dict[str, Any]] = item.get("Agents", {}) or {}

    root_leg                     = legs.get(root_id, {}) or {}
    customer_endpoint            = root_leg.get("CustomerEndpoint")
    initial_queue                = root_leg.get("QueueName")
    root_disconnect_reason       = root_leg.get("DisconnectReason")
    root_disconnect_ts           = root_leg.get("DisconnectTimestamp")
    inbound_agent                = root_leg.get("AgentUsername")
    inbound_transfer_ts          = iso_to_dt(root_leg.get("TransferCompletedTimestamp"))

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
        transfer_chain.append({"from": src, "to": dst, "at": dt_to_iso(when) if when else None})

    # Who ended (prefer transfer leg reason if ts matches call end)
    call_ended_dt = iso_to_dt(call_ended)
    end_reason = root_disconnect_reason
    if call_ended_dt and transfer_legs:
        for _, lg in transfer_legs:
            lg_dt = iso_to_dt(lg.get("DisconnectTimestamp"))
            if lg_dt and abs((lg_dt - call_ended_dt).total_seconds()) < 0.5:
                if lg.get("DisconnectReason"):
                    end_reason = lg.get("DisconnectReason")
                    break

    # Recording URLs (unique across legs)
    recording_urls: List[str] = []
    for _, lg in legs.items():
        url = lg.get("RecordingUrl")
        if isinstance(url, str) and url and url not in recording_urls:
            recording_urls.append(url)

    # -------------------------
    # Build structured events
    # -------------------------
    events: List[Dict[str, Any]] = []

    if call_started:
        events.append({"type": "CALL_STARTED", "ts": call_started})

    for a, info in agents.items():
        if info.get("firstJoin"):
            events.append({"type": "AGENT_JOINED", "ts": info["firstJoin"], "agent": a})

    for t in transfer_chain:
        if t.get("at"):
            events.append({"type": "TRANSFER_COMPLETED", "ts": t["at"], "from": t["from"], "to": t["to"]})

    for a, info in agents.items():
        if info.get("lastLeave"):
            # enrich with reason if leg matches
            leg_reason = None
            for _, lg in legs.items():
                if lg.get("AgentUsername") == a and lg.get("DisconnectTimestamp") == info["lastLeave"]:
                    leg_reason = lg.get("DisconnectReason"); break
            ev = {"type": "AGENT_LEFT", "ts": info["lastLeave"], "agent": a}
            if leg_reason:
                ev["reason"] = leg_reason
            events.append(ev)

    if call_ended:
        ev = {"type": "CALL_ENDED", "ts": call_ended}
        if end_reason:
            ev["reason"] = end_reason
        events.append(ev)

    # Sort ascending by time; tie-break with event priority
    def _key(ev: Dict[str, Any]):
        dt = iso_to_dt(ev.get("ts"))
        order = _EVENT_ORDER.get(ev.get("type"), 999)
        return (dt or datetime.max.replace(tzinfo=timezone.utc), order, ev.get("type"))
    events.sort(key=_key)

    # -------------------------
    # Human-readable timeline (format you asked)
    # -------------------------
    def render_line(ev: Dict[str, Any]) -> Optional[str]:
        ts_dt = iso_to_dt(ev.get("ts"))
        if not ts_dt:
            return None
        stamp = dt_to_iso(ts_dt)
        t = ev.get("type")

        if t == "CALL_STARTED":
            return f"{stamp} Customer connected the call."
        if t == "AGENT_JOINED":
            agent = ev.get("agent")
            # First join → answered
            if first_agent and agent == first_agent:
                return f"{stamp} Agent {agent} answered."
            return f"{stamp} Agent {agent} joined the call"
        if t == "TRANSFER_COMPLETED":
            return f"{stamp} Agent {ev.get('from')} transferred the call to Agent {ev.get('to')}"
        if t == "AGENT_LEFT":
            return f"{stamp} Agent {ev.get('agent')} left the call"
        if t == "CALL_ENDED":
            reason = ev.get("reason") or "UNKNOWN"
            return f"{stamp} Primary call ended and was disconnect by {reason}"
        return None

    text_lines: List[str] = []
    text_lines.append("Call Summary:\n")
    for ev in events:
        line = render_line(ev)
        if line:
            text_lines.append(line)

    if recording_urls:
        text_lines.append("\nRecording Links:")
        for u in recording_urls:
            # sample value of u: amazon-connect-d95997315caa/connect/xoriant-dev/CallRecordings/2026/02/02/26e3c0f7-d6e4-41c4-a906-4eddcbe52c86_20260202T17:12_UTC.wav
            bucket_name = u.split('/')[0]
            file_key = u.split(bucket_name + '/')[1]
            recording_url = SALESFORCE_S3_PRESIGNED_BASE_URL + 'fileKey=' + file_key + '&bucket=' + bucket_name
            text_lines.append(f"- {recording_url}")

    text_lines.append("\n\nTask Forms Summary:\n")
    for i, agent in agents.items():
        if agent.get("TaskFormId"):
            task_form_url = SALESFORCE_TASK_BASE_URL + agent['TaskFormId'] + '/view'
            text_lines.append(f" Task form filled by Agent, {i}: {task_form_url}")
        else:
            text_lines.append(f" Task form not filled by Agent, {i}")

    # Compose final text block (what we'll post as CaseComment)
    timeline_text_block = "\n".join(text_lines)

    # Pack
    result = {
        "meta": {
            "rootContactId": root_id,
            "caseId": case_id,
            "customerEndpoint": customer_endpoint,
            "initialQueue": initial_queue,
            "recordingUrls": recording_urls
        },
        "timeline": events,
        "timelineText": text_lines,
        "timelineTextBlock": timeline_text_block
    }
    return result

# -----------------------------
# Salesforce CaseComment poster
# -----------------------------

def post_case_comment(case_id: str, comment_body: str) -> Dict[str, Any]:
    """
    Create a Salesforce CaseComment with the given body.
    Uses shared get_access_token() for auth.
    """
    token_res = get_access_token()
    comment_url = f"{token_res['instance_url']}/services/data/v59.0/sobjects/CaseComment"

    payload = {
        "ParentId": case_id,
        "CommentBody": comment_body,
        "IsPublished": False  # internal; set True to expose to customers
    }

    resp = requests.post(
        comment_url,
        headers={
            "Authorization": f"Bearer {token_res['access_token']}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=20,
    )

    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Failed to create CaseComment: {resp.status_code} - {resp.text}")

    return resp.json()

# -----------------------------
# Lambda handler
# -----------------------------

def lambda_handler(event, context):
    """
    Expected event:
    {
      "rootContactId": "<UUID>"
    }

    Behavior:
    - Read the ConnectCallTimeline item
    - Build human-readable timeline (sorted)
    - Post as CaseComment to Salesforce
    - Return JSON with outcome and the same timeline text
    """
    # Log the event for traceability
    try:
        print("[EVENT] ", json.dumps(event, default=str))
    except Exception:
        print("[EVENT] <unprintable>")

    # Parse the input
    try:
        if isinstance(event, str):
            event = json.loads(event)
    except Exception:
        pass

    root_id = event.get("rootContactId") or event.get("RootContactId")
    if not root_id:
        msg = "Missing rootContactId in input"
        print(f"[ERROR] {msg}")
        return {"statusCode": 400, "body": json.dumps({"error": msg})}

    # Read the timeline item
    resp = TABLE.get_item(Key={"RootContactId": root_id}, ConsistentRead=True)
    item = resp.get("Item")
    if not item:
        msg = f"RootContactId '{root_id}' not found"
        print(f"[ERROR] {msg}")
        return {"statusCode": 404, "body": json.dumps({"error": msg})}

    native = to_native(item)
    result = build_timeline_payload(native)

    # Pretty print to logs (useful for audits)
    print("=" * 80)
    print(f"[TIMELINE RESULT] RootContactId={root_id}")
    print("-" * 80)
    try:
        print(json.dumps(result, indent=2, sort_keys=False, default=str))
    except Exception as e:
        print(f"[WARN] Could not pretty-print result: {e}")
        print(str(result))
    print("=" * 80)
    print(result.get("timelineTextBlock", ""))

    # Post to Salesforce CaseComment
    case_id = result["meta"].get("caseId")
    if not case_id:
        msg = f"No CaseId present for RootContactId={root_id}; cannot post CaseComment."
        print(f"[ERROR] {msg}")
        return {"statusCode": 409, "body": json.dumps({"error": msg, "timelineTextBlock": result.get('timelineTextBlock')})}

    try:
        sf_res = post_case_comment(case_id, result["timelineTextBlock"])
        print(f"[SF] CaseComment created on Case {case_id}: {sf_res}")
        body = {
            "message": "Timeline posted to Salesforce CaseComment",
            "caseId": case_id,
            "rootContactId": root_id,
            "salesforceResult": sf_res,
            "timelineTextBlock": result["timelineTextBlock"]
        }
        return {"statusCode": 200, "body": json.dumps(body)}
    except Exception as e:
        print(f"[ERROR] Failed to post CaseComment for Case {case_id}: {e}")
        return {
            "statusCode": 502,
            "body": json.dumps({
                "error": str(e),
                "caseId": case_id,
                "rootContactId": root_id,
                "timelineTextBlock": result.get("timelineTextBlock")
            })
        }
