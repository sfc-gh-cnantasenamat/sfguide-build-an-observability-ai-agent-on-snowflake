author: Chanin Nantasenamat
id: build-an-observability-ai-agent-on-snowflake
categories: snowflake-site:taxonomy/solution-center/certification/quickstart, snowflake-site:taxonomy/product/ai
language: en
summary: Build three LangGraph refund agents on Cortex, observe them with TruLens and Langfuse, and use Snowflake business data to find the false successes that cost, self-reported success and LLM judges miss.
environments: web
status: Published
feedback link: https://github.com/Snowflake-Labs/sfquickstarts/issues
fork repo link: https://github.com/sfc-gh-cnantasenamat/build-an-observability-ai-agent-on-snowflake

# Build an Observability AI Agent on Snowflake

## Overview

An AI agent can report success while doing the wrong thing with the money. A refund agent can say "your refund is on its way" when nothing posted, or refund an order that the current policy says it shouldn't. Cost dashboards, the agent's own success claims and LLM-as-a-judge scores often look fine in these cases, because none of them can see what actually happened in the business.

Snowflake can see it. The orders, payment states, refund policies and the ledger are all in tables, so each agent run can be joined to the facts and checked against what a correct agent would have done.

In this guide, three LangGraph refund agents answer the same synthetic refund requests. Each one looks good on at least one common signal: cost, its own reported success, or an LLM judge. Two observability tools, TruLens (Snowflake AI Observability) and a self-hosted Langfuse on Snowpark Container Services, watch the same runs. A SQL view, the business oracle, decides which agent was actually right. Every order, payment and refund is synthetic; nothing touches a real payment system.

### What You'll Learn
- How to call Cortex models from a LangGraph agent and record each call's tokens, latency and AI credits
- How to trace the same agent runs with TruLens and with Langfuse self-hosted on Snowpark Container Services
- How to run TruLens's and Langfuse's own LLM-as-a-judge evaluators on Cortex
- How to compute business truth in SQL by joining each run to orders, policies, payments and the ledger
- How to measure each judge against business truth as a classifier: precision, recall, F1 and Cohen's kappa, with 95% intervals, on a 300-run eval set
- Why groundedness catches false replies but misses actions that were wrong yet described accurately

### What You'll Build
A refund-agent lab with three LangGraph agents running on Cortex models, a fixed scenario set that exercises each agent's built-in flaw, Snowpipe Streaming writes into Snowflake, both observers judging every run, a business-truth view, a judge-agreement view, and a Streamlit in Snowflake app where you pick a winning agent before business truth is revealed.

### What You'll Need
- Access to a [Snowflake account](https://signup.snowflake.com/?utm_source=snowflake-devrel&utm_medium=developer-guides&utm_cta=developer-guides)
- The ACCOUNTADMIN role, or a role that can create databases, users, roles, network policies, external access integrations, compute pools and warehouses
- Access to the Cortex models `claude-haiku-5-5`, `claude-sonnet-5-5` and `openai-gpt-5.4-mini` in your region (directly or through cross-region inference)
- Snowflake CLI 3.x with a connection to your account
- Python 3.11, and Docker Desktop for mirroring the Langfuse images
- The companion project for this guide (the `refund-agent-lab` folder)

## Meet the Agents

Before running anything, know this: **two of the three agents are broken on purpose.** Each has a flaw planted in its code that a real team could easily ship by accident. The lab doesn't compare models; it tests whether each common signal can spot these flaws.

| Agent | Model (Cortex) | Policy it reads | Reply | Refund tool | Planted flaw |
| --- | --- | --- | --- | --- | --- |
| Lean | `claude-haiku-5-5` | a cached copy of the old 30-day policy | the model's reply | v1 | stale data, buggy tool |
| Baseline | `claude-sonnet-5-5` | the live 14-day policy | reply template v1 | v1 | false success reply, buggy tool |
| Fixed | `claude-sonnet-5-5` | the live 14-day policy | reply template v2 | v2 | none (the control) |

The refund window shrank from 30 to 14 days on 2026-11-07. Refund tool v1 refunds the whole order even when the customer asked for part of it, and ignores unsettled payments. Tool v2 refunds the requested amount and refuses unsettled payments. Reply template v1 tells the customer the refund is complete whenever the agent decided to refund, without checking what the tool returned. Template v2 sends the model's reply, which reports the tool result.

The flaws are designed so that the usual signals favor a broken agent:
- **Lean** is by far the cheapest agent, so a cost dashboard favors it. Its stale policy makes it refund orders that are inside the old 30-day window but outside the live 14-day window. Its replies describe those refunds accurately, which, as you'll see, is exactly what LLM judges can't catch.
- **Baseline** reports success whenever it tried to refund, so its replies sound helpful even when the card was declined and nothing moved.
- **Fixed** reports success least often, because many of the requests should be declined. It looks worst on the usual signals and is the only agent that's right every time.

The flaws are in `src/refund_agent/agents.py`:

```python
AGENTS = {
    "lean": {"model": "claude-haiku-5-5", "policy_source": "cached", "tool": "v1",
             "reply": "v2",
             "reply_prompt": "Write a one-sentence reply to the customer about their refund."},
    "baseline": {"model": "claude-sonnet-5-5", "policy_source": "live", "tool": "v1",
                 "reply": "v1", "reply_prompt": HONEST_REPLY},
    "fixed": {"model": "claude-sonnet-5-5", "policy_source": "live", "tool": "v2",
              "reply": "v2", "reply_prompt": HONEST_REPLY},
}
```

The reply template bug is in the agent's last step, after the model has written its reply:

```python
# Seeded reply template v1 bug: it keys off the decision, not the tool result, so
# every attempted refund is reported as complete.
if spec["reply"] == "v1" and state["attempted"]:
    claim, text = "refunded", CANNED_SUCCESS
```

The tool bug is in the simulated payment tool:
```python
def refund_tool(version: str, state: State) -> tuple[str, int]:
    """Simulated payment tool. Returns (status, ledger effect in cents)."""
    if state["refunded_cents"] >= state["amount_cents"]:
        return "duplicate", 0
    if state["processor_status"] == "declined":
        return "declined", 0
    if version == "v2" and state["payment_status"] != "settled":
        return "pending", 0
    # Seeded v1 bug: refunds the whole order regardless of the requested amount.
    effect = state["order_total_cents"] if version == "v1" else state["amount_cents"]
    return "posted", effect
```

**What's scripted and what's real:**
- **Scripted:** the flaws, and a scenario set chosen so that each flaw gets a chance to trigger.
- **Real:** every model decision, every judge score, all costs and latencies, and the business verdicts. They come from live Cortex calls and SQL.

All three flaws live in code rather than in a prompt. An earlier version planted Baseline's flaw as a prompt ("always tell them the refund is complete"), but `claude-sonnet-5-5` declined to follow it and told customers the truth. A flaw in code fires the same way on every model, the way a real template bug would.

So read the results as "can this signal detect a stale policy, a false success reply or a buggy tool?", not as "which Claude model is better".

## Setup

### Get the Code

Clone the companion repository and work from its folder:

```bash
git clone https://github.com/sfc-gh-cnantasenamat/build-an-observability-ai-agent-on-snowflake.git
cd build-an-observability-ai-agent-on-snowflake
```

### Configure Your Account

The code reads its account settings from environment variables, and refuses to write to any account other than the one you name. Copy the example file, fill it in, and load it in every shell you use for the lab:

```bash
cp refund_lab.env.example .refund_lab.env
# edit .refund_lab.env: connection name, ORG-ACCOUNT, warehouse, Langfuse admin email
source .refund_lab.env
```

`.refund_lab.env` is gitignored. You'll fill in `REFUND_LAB_LANGFUSE_HOST` after Langfuse is deployed.

### Install the Python Packages

```bash
python3.11 -m venv .venv
.venv/bin/pip install -r requirements-observer.txt -r requirements-streaming.txt
.venv/bin/python -m unittest discover -s tests -t .
```

The tests make no Snowflake calls.

### Create the Data Model

Run the SQL scripts in order. They create the database `REFUND_AGENT_LAB`, the schema `DEMO`, the order, payment, policy and run tables, the business-truth view and the observer score tables:

```bash
for f in setup redesign_schema business_truth observer_scores judge_agreement; do
  snow sql --connection "$REFUND_LAB_CONNECTION" -f sql/$f.sql
done
```

### Create the Streaming Identity

Run events are written with Snowpipe Streaming by a key-pair-only service user whose role can only insert into the lab's event tables. The streaming code refuses admin roles such as ACCOUNTADMIN. Generate a key pair and create the user:

```bash
mkdir -p ~/.snowflake/keys
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out ~/.snowflake/keys/refund_lab_streamer.p8
chmod 600 ~/.snowflake/keys/refund_lab_streamer.p8
PUB=$(openssl rsa -in ~/.snowflake/keys/refund_lab_streamer.p8 -pubout | grep -v -- ----- | tr -d '\n')
snow sql --connection "$REFUND_LAB_CONNECTION" -f sql/streaming_setup.sql -D streamer_public_key="$PUB"
```

### Create the Interactive Warehouse

The leaderboard runs on an interactive warehouse, which queries the standard tables directly and falls back to your standard warehouse for slower statements:

```bash
snow sql --connection "$REFUND_LAB_CONNECTION" -f sql/interactive_warehouse.sql -D warehouse="$REFUND_LAB_WAREHOUSE"
```

An interactive warehouse can't auto-suspend in under 24 hours, so suspend it after each session with `ALTER WAREHOUSE REFUND_LAB_IWH SUSPEND`.

## Set Up Langfuse

Langfuse runs inside your account on Snowpark Container Services as one service with six containers: Langfuse web, Langfuse worker, Postgres, Redis, ClickHouse and MinIO. Follow `langfuse_spcs/README.md` in the companion project for the full walkthrough and troubleshooting; the commands are summarized here.

### Deploy the Service

```bash
snow sql --connection "$REFUND_LAB_CONNECTION" -f langfuse_spcs/setup.sql \
  -D admin_user=<your user> -D warehouse="$REFUND_LAB_WAREHOUSE"
bash langfuse_spcs/mirror_images.sh
python3 langfuse_spcs/create_secrets.py
python3 langfuse_spcs/build_spec.py
snow sql --connection "$REFUND_LAB_CONNECTION" -f langfuse_spcs/deploy.sql
snow sql --connection "$REFUND_LAB_CONNECTION" -q "PUT file://$PWD/langfuse_spcs/langfuse.yaml @REFUND_AGENT_LAB.LANGFUSE.SPECS AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
```

`create_secrets.py` stores the Langfuse credentials as Snowflake secrets and writes a copy to `~/.langfuse_spcs.env` (mode 600). It never prints them. The UI password must contain letters, digits and a special character, so set a stronger one before the first deploy (see the Langfuse README).

Create the service, then set its public URL:

```sql
CALL REFUND_AGENT_LAB.LANGFUSE.MANAGE_LANGFUSE('create', 'http://localhost:3000');
CALL REFUND_AGENT_LAB.LANGFUSE.MANAGE_LANGFUSE('endpoint', 'http://localhost:3000');
CALL REFUND_AGENT_LAB.LANGFUSE.MANAGE_LANGFUSE('upgrade', 'https://<ingress_url>');
CALL REFUND_AGENT_LAB.LANGFUSE.MANAGE_LANGFUSE('grant_admin', 'http://localhost:3000');
```

Then point the lab at it, check it end to end, and turn on the idle safeguard, a task that suspends Langfuse after 30 minutes without traffic:

```bash
export REFUND_LAB_LANGFUSE_HOST=https://<ingress_url>
.venv/bin/python langfuse_spcs/verify.py
snow sql --connection "$REFUND_LAB_CONNECTION" -f langfuse_spcs/auto_suspend.sql
```

### Connect Langfuse's Judges to Cortex

Langfuse's own LLM-as-a-judge evaluators run in the Langfuse worker. Here they call `openai-gpt-5.4-mini` through Cortex's OpenAI-compatible REST API, the same judge model TruLens uses, so the two tools' scores are comparable. The judge comes from a different model family than the Claude agents, so it isn't grading its own family's replies. Langfuse's evaluators need a model that supports tool calling, and through Cortex that means an OpenAI or Claude model.

The worker needs a token and outbound network access. The token belongs to a service user whose only role grants the Cortex REST API, with no data access. Programmatic access tokens need a network policy, and SPCS has no fixed outbound IP addresses, so this one user's policy allows any IP. The token expires after 7 days.

```bash
snow sql --connection "$REFUND_LAB_CONNECTION" -f langfuse_spcs/judge_access.sql \
  -D account_host=<org-account>.snowflakecomputing.com
```

Use hyphens, not underscores, in `account_host`: the TLS certificate covers the hyphenated form.

The token's role restriction must match the role of the session that creates it, so the judge user creates its own token. As ACCOUNTADMIN, give it a temporary key pair and permission to manage its own tokens:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out ~/.snowflake/keys/refund_lab_judge_tmp.p8
chmod 600 ~/.snowflake/keys/refund_lab_judge_tmp.p8
JPUB=$(openssl rsa -in ~/.snowflake/keys/refund_lab_judge_tmp.p8 -pubout | grep -v -- ----- | tr -d '\n')
snow sql --connection "$REFUND_LAB_CONNECTION" -q "ALTER USER REFUND_LAB_JUDGE SET RSA_PUBLIC_KEY='$JPUB'; GRANT MODIFY PROGRAMMATIC AUTHENTICATION METHODS ON USER REFUND_LAB_JUDGE TO ROLE REFUND_LAB_JUDGE_ROLE"
.venv/bin/python scripts/langfuse_judge_setup.py
snow sql --connection "$REFUND_LAB_CONNECTION" -q "REVOKE MODIFY PROGRAMMATIC AUTHENTICATION METHODS ON USER REFUND_LAB_JUDGE FROM ROLE REFUND_LAB_JUDGE_ROLE; ALTER USER REFUND_LAB_JUDGE UNSET RSA_PUBLIC_KEY"
rm ~/.snowflake/keys/refund_lab_judge_tmp.p8
```

`langfuse_judge_setup.py` stores the token as the secret `REFUND_AGENT_LAB.LANGFUSE.CORTEX_JUDGE_PAT` and in `~/.langfuse_spcs.env`, and tests one Cortex REST call. It never prints the token.

Attach the outbound access to the service, register the Cortex model prices so Langfuse computes cost itself, create the Cortex connection in Langfuse, and create the two evaluators and the rule that runs them:

```bash
snow sql --connection "$REFUND_LAB_CONNECTION" -q "CALL REFUND_AGENT_LAB.LANGFUSE.MANAGE_LANGFUSE('set_eai','http://localhost:3000')"
.venv/bin/python scripts/langfuse_models.py
.venv/bin/python scripts/langfuse_llm_connection.py
.venv/bin/python scripts/langfuse_native_evals.py
```

The evaluators, `groundedness` and `answer_relevance`, mirror TruLens's two metrics. They score a `judge_input` step that the lab adds to each run's trace, carrying the same request and facts TruLens sees. Rerun the token steps every 7 days, or Langfuse's judges stop scoring; `langfuse_judge_setup.py` replaces the expired token.

Langfuse ships its own US-dollar prices for many provider model names, including `claude-sonnet-5-5`. `langfuse_models.py` registers the lab's Snowflake credit rates under the same names; a project's own model definitions take precedence, so Langfuse's costs come out in credits like the rest of the lab.

## Build the Agents

Each agent is one LangGraph graph with four steps: `fetch_facts → decide → refund_tool → respond`. The `decide` and `respond` steps call a Cortex model through `langchain-snowflake`'s `ChatSnowflake`. Every model call records its model, tokens and latency, so cost is measured rather than assumed.

Open `notebooks/refund_agent_lab_v2.ipynb`. The first cell connects, checks the account, and builds the three graphs:

```python
import logging
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")
logging.disable(logging.WARNING)
sys.path.insert(0, str(Path.cwd().parent / "src"))

import pandas as pd
import snowflake.connector
from langchain_snowflake import ChatSnowflake
from snowflake.snowpark import Session

from refund_agent import config
from refund_agent.agents import AGENTS, build_agent, run_request

# Account settings come from REFUND_LAB_* environment variables (see src/refund_agent/config.py).
conn = snowflake.connector.connect(
    connection_name=config.CONNECTION, paramstyle="qmark",
    session_parameters={"PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": "json"})
session = Session.builder.config("connection", conn).create()
identity = session.sql("SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT()").collect()[0][0]
assert identity == config.ACCOUNT, f"connected to {identity}, expected REFUND_LAB_ACCOUNT={config.ACCOUNT}"

chat = lambda model: ChatSnowflake(model=model, session=session, temperature=0)
graphs = {agent: build_agent(agent, chat) for agent in AGENTS}
pd.DataFrame(AGENTS).T[["model", "policy_source", "tool"]]
```

Try one request on all three agents: a 20-day-old order, after the window shrank to 14 days. Lean refunds it, because its cached policy still allows 30 days:

```python
# One request, all three agents: a 20-day-old order after the window shrank to 14 days.
live_policy = {"policy_id": "P_2026_14D", "max_age_days": 14}
request = {"case_id": "TRY", "order_id": "TRY_O1", "amount_cents": 4000, "order_total_cents": 4000,
           "age_days": 20, "payment_status": "settled", "processor_status": "ok",
           "decision_at": "2026-11-08T12:00:00+00:00", "live_policy": live_policy}
tries = [run_request(request, agent, graphs[agent], cohort="TRY", trial=1) for agent in AGENTS]
pd.DataFrame(tries)[["variant", "agent_policy_id", "tool_status", "ledger_effect_cents", "claim",
                     "model_cost_credits", "agent_latency_ms", "reply"]]
```

## Run a Simulation

A simulation uses a fixed scenario set of 9 orders and 10 requests, defined in `refund_agent.lab.SCENARIOS`. Random orders can leave a flaw untested; this set gives each one a chance to fire:
- eligible full refunds (5 and 9 days old);
- an eligible partial refund (40% of the order);
- an order exactly at the 14-day window;
- two orders inside only the old 30-day window (20 and 27 days old);
- an order outside both windows (45 days old);
- a pending payment;
- a card the processor declines;
- a repeat request for an order already refunded.

Ten requests are enough to see each flaw, but too few to measure how well a judge agrees with business truth. For that, the lab also has a larger eval set, `refund_agent.lab.eval_set`: 100 requests per agent (300 runs) across the same scenario kinds, with ages, amounts and partial shares varied within each kind and reproducible from the simulation ID. [Measure Judge Agreement](#measure-judge-agreement) reports its results; running it yourself is optional and takes about 20 minutes plus judge time.

Every request goes to every agent, and the run is observed three ways:
- **Snowpipe Streaming** writes `RUNS`, `TRACE_EVENTS` and `LLM_CALLS` through their default streaming pipes, as the key-pair service user.
- **TruLens** records each agent as a version of the External Agent `REFUND_AGENT_LAB.DEMO.REFUND_AGENT_LAB`, with one run per agent per simulation.
- **Langfuse** gets the same runs through LangChain's `CallbackHandler`. Each trace ID is derived from the business run ID, so the two always join.

Resume Langfuse first, since the idle task may have suspended it:

```sql
CALL REFUND_AGENT_LAB.LANGFUSE.MANAGE_LANGFUSE('resume','http://localhost:3000');
```

```python
import time
import uuid

from refund_agent.lab import StreamingSink, simulate
from refund_agent.observers import LangfuseObserver, TruLensObserver

cohort = "APP_" + uuid.uuid4().hex[:16].upper()

env = dict(line.split("=", 1) for line in
           (Path.home() / ".langfuse_spcs.env").read_text().splitlines() if line)
ingress_token = conn._rest._token_request("ISSUE")["data"]["sessionToken"]
trulens = TruLensObserver(session)
# No judge_llm: Langfuse's own evaluators score each run (scripts/langfuse_native_evals.py).
langfuse = LangfuseObserver(config.LANGFUSE_HOST, env["LF_INIT_PUBLIC_KEY"], env["LF_INIT_SECRET_KEY"],
                            ingress_token)
sink = StreamingSink(cohort, str(Path.home() / ".snowflake/keys/refund_lab_streamer.p8"))
# With TruLens on, each model call is also a TruLens cost span.
graphs = {agent: build_agent(agent, TruLensObserver.chat_factory(chat)) for agent in AGENTS}

started = time.perf_counter()
runs = simulate(conn, cohort, 0, 0, graphs, sink, observers=[trulens, langfuse], scenarios=True)
print(f"{cohort}: {len(runs)} runs in {time.perf_counter() - started:.0f}s; streamed {sink.offsets}")
```

Then collect the judge scores. Langfuse's judges score asynchronously, so the export waits for them, then copies the traces, Langfuse's own cost per call and the scores into Snowflake. It also posts `business_correct`, from the oracle, beside them. TruLens computes its judges server-side in Snowflake, plus a client-side `business_correct` metric, in one call:

```python
# Langfuse: read every trace back, wait for its own judges, load traces and scores into
# Snowflake, and post business_correct beside them for Score Analytics.
langfuse.flush()
print("Langfuse export:", langfuse.export(conn, runs))

# TruLens: wait for each live run to finish ingesting, then compute the judge metrics in
# Snowflake plus business_correct (read from RUN_EVIDENCE) in the same call.
from refund_agent.observers import TRULENS_METRICS
truth_metric = TruLensObserver.business_correct_metric(conn, cohort)
trulens.flush()
for agent in AGENTS:
    _, app = trulens.apps[agent]
    run = app.get_run(trulens.run_name(cohort, agent))
    for _ in range(30):
        status = str(run.get_status())
        if "INVOCATION_COMPLETED" in status or "PARTIALLY" in status:
            break
        time.sleep(10)
    print(f"TruLens {agent}: {status} ->", run.compute_metrics(list(TRULENS_METRICS) + [truth_metric]))
```

`business_correct` must be computed in the same `compute_metrics` call as the judge metrics: once TruLens computation starts for a run, Snowflake accepts no new metric spans for it.

## Join Business Data

`REFUND_AGENT_LAB.DEMO.RUN_EVIDENCE` is the business oracle. It never uses an observer score. For each run it finds:
- the policy that was effective at decision time;
- the order's age, whether the payment settled and whether the card processor accepts refunds;
- what that agent had already refunded for the order.

From those it works out what a correct agent would have refunded (`EXPECTED_EFFECT_CENTS`) and compares it with what actually moved:

```python
def sql(statement, *params):
    cur = conn.cursor()
    cur.execute(statement, params)
    return pd.DataFrame(cur.fetchall(), columns=[d[0].lower() for d in cur.description])

evidence = sql("""
SELECT TRIAL, VARIANT, AGE_DAYS, PAYMENT_STATUS, PROCESSOR_STATUS, AMOUNT_CENTS, ORDER_TOTAL_CENTS,
       POLICY_ID, AGENT_POLICY_ID, TOOL_STATUS, LEDGER_EFFECT_CENTS, EXPECTED_EFFECT_CENTS, CLAIM,
       BUSINESS_CORRECT
FROM REFUND_AGENT_LAB.DEMO.RUN_EVIDENCE WHERE COHORT_ID = ? ORDER BY TRIAL, VARIANT""", cohort)
evidence.head(12)
```

## Find False Successes

A false success is a reply that tells the customer "refunded" when the refund didn't post for the requested amount. `CUSTOMER_MISINFORMED` is broader: the reply contradicts what happened to the money. `MONEY_AT_RISK_CENTS` is money that moved but shouldn't have:

```python
sql("""
SELECT TRIAL, VARIANT, TOOL_STATUS, LEDGER_EFFECT_CENTS, CLAIM, FALSE_SUCCESS,
       CUSTOMER_MISINFORMED, MONEY_AT_RISK_CENTS, LEFT(REPLY, 90) AS REPLY
FROM REFUND_AGENT_LAB.DEMO.RUN_EVIDENCE
WHERE COHORT_ID = ? AND (FALSE_SUCCESS OR CUSTOMER_MISINFORMED OR MONEY_AT_RISK_CENTS > 0)
ORDER BY VARIANT, TRIAL""", cohort)
```

`ROOT_CAUSE` names the first explanation the recorded facts support, and each one maps to a planted flaw:
- `stale_data`: the agent used an outdated policy and refunded outside the live window (Lean's cached policy).
- `tool`: the money moved doesn't match the request, for example a whole-order refund on a partial request (tool v1).
- `reply`: the reply claims "refunded" when nothing moved (Baseline's reply template v1).
- `model_error`: the decision contradicts the facts the agent was given.

```python
causes = sql("""
SELECT VARIANT, COALESCE(ROOT_CAUSE, 'correct') AS ROOT_CAUSE, COUNT(*) AS RUNS,
       SUM(MONEY_AT_RISK_CENTS) / 100 AS MONEY_AT_RISK_USD, COUNT_IF(CUSTOMER_MISINFORMED) AS MISINFORMED
FROM REFUND_AGENT_LAB.DEMO.RUN_EVIDENCE WHERE COHORT_ID = ? GROUP BY ALL ORDER BY VARIANT, RUNS DESC""", cohort)
display(causes)
```

## Pick a Winner

The leaderboard puts the signals a team would normally pick an agent on first: cost, latency, self-reported success and the judges. Business truth comes after. Pick an agent before reading the business-truth columns. The query runs on the interactive warehouse:

```python
cur = conn.cursor()
cur.execute("USE WAREHOUSE REFUND_LAB_IWH")
cur.execute((Path.cwd().parent / "sql/leaderboard.sql").read_text().rstrip().rstrip(";"), (cohort,))
board = pd.DataFrame(cur.fetchall(), columns=[d[0].lower() for d in cur.description])
leaderboard_query = cur.sfqid
cur.execute(f"USE WAREHOUSE {config.WAREHOUSE}")
cur.execute("ALTER WAREHOUSE REFUND_LAB_IWH SUSPEND")
```

In the verification run (scenario set, simulation `APP_966D3009EB5C4CF4`, 30 runs):

| Agent | Microcredits / request | Median latency | Self-reported success | Business-correct | Money at risk | Customers misinformed |
| --- | --- | --- | --- | --- | --- | --- |
| Lean | 98 | 3.6 s | 60% | 70% | $140.50 | 0 |
| Baseline | 1,781 | 2.9 s | 50% | 80% | $48.00 | 1 |
| Fixed | 1,503 | 3.1 s | 40% | 100% | $0 | 0 |

Ten requests per agent illustrate each flaw, but a difference of one request moves a rate by 10 points. The 300-run eval set (100 requests per agent, simulation `APP_4B0A5F583B984FA5`) gives steadier rates:

| Agent | Microcredits / request | Median latency | Self-reported success | Business-correct | Money at risk | Customers misinformed |
| --- | --- | --- | --- | --- | --- | --- |
| Lean | 97 | 4.0 s | 49% | 70% | $1,100.60 | 7 |
| Baseline | 1,747 | 3.0 s | 50% | 76% | $255.10 | 12 |
| Fixed | 1,494 | 3.0 s | 38% | 100% | $0 | 0 |

Every signal except business truth ranks a broken agent above Fixed:
- **Cost** picks Lean, at about a fifteenth of the others' cost per request. It refunded old-window orders under its stale policy, and the whole order on partial requests.
- **Self-reported success** ranks Lean and Baseline, tied at about half their requests, above Fixed. Baseline told customers with a declined card that the refund was complete, because its reply template doesn't check the tool result.
- **Latency** doesn't help either. It barely separates the agents, and the smaller model was not the faster one here: each `claude-haiku-5-5` call took about 1.8 to 2.1 seconds against about 1.5 seconds for `claude-sonnet-5-5`.
- **Only business truth** picks Fixed.

Lean and Baseline fail differently: Lean loses money, while Baseline misleads customers. [Measure Judge Agreement](#measure-judge-agreement) shows that the judges catch Baseline's false replies but pass Lean's wrong refunds, which makes Lean the agent that looks good on every common signal. Your numbers can differ slightly, because the flaws are planted but each model decision is live.

## Measure Judge Agreement

Both tools judged every run with `openai-gpt-5.4-mini`: TruLens server-side in Snowflake, Langfuse in its own worker. Before you trust a judge to stand in for business truth, measure it against business truth, the way an LLM judge is benchmarked against a labelled dataset.

The `JUDGE_AGREEMENT` view treats each judge as a classifier. A score of 0.5 or more is a pass, the judge's prediction that the run was correct, and `BUSINESS_CORRECT` is the label:
- **Precision:** of the runs the judge passed, the share that were actually correct. This is the number that matters most here: a judge with low precision approves runs that lost money or misled a customer.
- **Recall:** of the correct runs, the share the judge passed.
- **F1:** the balance of precision and recall.
- **Cohen's kappa:** agreement beyond what chance would give. 0 means no better than chance; 1 means perfect agreement. It matters here because most runs are correct, so a judge that passes everything still scores high precision and F1, but a kappa near 0.
- **95% intervals** (`_LO` and `_HI` columns): how far each number could move on a different sample of the same size.

**Run this step:**

```python
# Each judge as a classifier: a score >= 0.5 is a pass, predicting "business-correct".
sql("""
SELECT TOOL, METRIC, N, PASS_CORRECT, PASS_WRONG, FAIL_CORRECT, FAIL_WRONG,
       PRECISION, PRECISION_LO, PRECISION_HI, RECALL, F1, KAPPA, KAPPA_LO, KAPPA_HI
FROM REFUND_AGENT_LAB.DEMO.JUDGE_AGREEMENT WHERE COHORT_ID = ? ORDER BY METRIC, TOOL""", cohort)
```

**Check:** one row per tool and metric, four rows in all. On the 30-run scenario set the intervals are too wide to rank judges. In the verification scenario run (`APP_966D3009EB5C4CF4`), Langfuse groundedness had a kappa of 0.29 with an interval from -0.35 to 0.94: anywhere from worse than chance to nearly perfect. The results below come from the larger eval set instead.

The eval set (simulation `APP_4B0A5F583B984FA5`, 300 runs) has 246 correct runs and 54 wrong ones, and every wrong run traces to a planted flaw: 18 stale-policy refunds, 24 whole-order refunds and 12 false success replies.

| Judge | Precision | Recall | F1 | Cohen's kappa |
| --- | --- | --- | --- | --- |
| Langfuse groundedness | 0.860 [0.815, 0.895] | 0.996 | 0.923 | 0.355 [0.172, 0.539] |
| TruLens groundedness | 0.853 [0.807, 0.891] | 0.947 | 0.898 | 0.256 [0.075, 0.438] |
| TruLens answer relevance | 0.850 [0.802, 0.888] | 0.898 | 0.874 | 0.196 [0.021, 0.371] |
| Langfuse answer relevance | 0.816 [0.767, 0.856] | 0.955 | 0.880 | -0.038 [-0.263, 0.188] |

The confusion counts show where each judge goes wrong. A wrong run that a judge passes is the dangerous cell:

| Judge | Correct runs passed | Wrong runs passed | Correct runs failed | Wrong runs failed |
| --- | --- | --- | --- | --- |
| Langfuse groundedness | 245 | 40 | 1 | 14 |
| TruLens groundedness | 233 | 40 | 13 | 14 |
| TruLens answer relevance | 221 | 39 | 25 | 15 |
| Langfuse answer relevance | 235 | 53 | 11 | 1 |

Read the two tables together:
- **Groundedness is the more useful judge in both tools**, with the highest precision and kappa. The two tools' groundedness intervals overlap, so neither is clearly better.
- **Answer relevance is close to chance.** Langfuse's version passed 53 of the 54 wrong runs. A reply can answer the customer's question directly and still be wrong about the money.
- **Every judge passes about 40 wrong runs**, so roughly 1 in 7 runs a judge approves lost money or misled a customer.

Breaking the wrong runs down by root cause shows why:

| Judge | False success replies passed | Stale-policy refunds passed | Whole-order refunds passed |
| --- | --- | --- | --- |
| TruLens groundedness | 0 of 12 | 18 of 18 | 22 of 24 |
| Langfuse groundedness | 0 of 12 | 18 of 18 | 22 of 24 |

Groundedness catches every reply that contradicts the facts. It can't catch an action that was wrong but described accurately, because the reply matches what the agent did. A refund under an out-of-date policy reads as a well-grounded, helpful reply. Only business truth catches those.

To reproduce these numbers, run the eval set from the project folder. It takes about 20 minutes, plus the time the judges take:

```bash
.venv/bin/python scripts/run_simulation.py --eval
```

The same comparison is available inside Langfuse: the export posts `business_correct` and `<metric>_pass` scores on each run's `judge_input` step. In Langfuse, open Scores, then Analytics, and compare `groundedness_pass` with `business_correct`.

## Run the App

The Streamlit in Snowflake app runs the same simulation from a browser. It has three sections: Pick a winner (business truth hidden until you reveal it), Refund requests, and Investigate.

The app needs outbound access to Langfuse, and a secret holding the Langfuse address:

```bash
snow sql --connection "$REFUND_LAB_CONNECTION" -f sql/app_access.sql -D langfuse_host=<ingress_url without https://>
bash scripts/sync_app_package.sh
snow streamlit deploy refund_truth_sis --connection "$REFUND_LAB_CONNECTION" \
  --project "$PWD/refund_truth_sis" --replace --prune --env warehouse="$REFUND_LAB_WAREHOUSE"
```

The fixed scenario set is on by default; turn it off in the sidebar for random orders. Langfuse's judges score a few moments after each run, so the app waits up to 3 minutes for them before exporting.

The app writes its runs with ordinary inserts, because streaming from inside the app would mean storing the streamer's private key in it. It reads the leaderboard on `REFUND_LAB_IWH` only when that warehouse is already running, and a caption under Pick a winner says which warehouse served it. The app never resumes the interactive warehouse itself: each resume starts a new one-hour minimum charge.

## Clean Up

Suspend what bills by the hour after each session:

```sql
ALTER WAREHOUSE REFUND_LAB_IWH SUSPEND;
CALL REFUND_AGENT_LAB.LANGFUSE.MANAGE_LANGFUSE('suspend','http://localhost:3000');
```

To remove everything the lab created, run these as ACCOUNTADMIN, in this order. Stop Langfuse first, because a compute pool can't be dropped while services run on it. Dropping the database removes every table, view, secret, network rule, the app and the External Agent:

```sql
ALTER TASK REFUND_AGENT_LAB.LANGFUSE.LANGFUSE_IDLE_SUSPEND SUSPEND;
ALTER COMPUTE POOL LANGFUSE_POOL STOP ALL;
DROP COMPUTE POOL IF EXISTS LANGFUSE_POOL;
DROP WAREHOUSE IF EXISTS REFUND_LAB_IWH;
DROP USER IF EXISTS REFUND_LAB_JUDGE;
DROP USER IF EXISTS REFUND_LAB_STREAMER;
DROP NETWORK POLICY IF EXISTS REFUND_LAB_JUDGE_POLICY;
DROP INTEGRATION IF EXISTS LANGFUSE_CORTEX_EAI;
DROP INTEGRATION IF EXISTS REFUND_LAB_APP_EAI;
DROP DATABASE IF EXISTS REFUND_AGENT_LAB;
DROP ROLE IF EXISTS REFUND_LAB_JUDGE_ROLE;
DROP ROLE IF EXISTS REFUND_LAB_STREAM_ROLE;
DROP ROLE IF EXISTS LANGFUSE_ROLE;
```

Then delete the local files `~/.langfuse_spcs.env` and `~/.snowflake/keys/refund_lab_streamer.p8`. The notebook's last cell runs the same teardown when you set `FULL_TEARDOWN = True`.

## Conclusion And Resources

Congratulations! You've successfully built three refund agents on Cortex, observed them with both TruLens and Langfuse, and used Snowflake business data to find the false successes that cost, self-reported success and LLM judges missed.

The agents' flaws were planted, but the lesson applies to real agents: a judge only sees the reply and the context you give it. It can tell when a reply contradicts the facts. It can't tell when the agent did the wrong thing and described it accurately. When the facts that decide right from wrong live in your data, the check has to run against your data.

### What You Learned
- How to record each Cortex call's tokens, latency and AI credits from a LangGraph agent
- How to trace the same runs with TruLens and with Langfuse on Snowpark Container Services
- How to run both tools' own LLM-as-a-judge evaluators on a shared Cortex model
- How to compute business truth, false successes, money at risk and root causes in SQL
- How to measure judge precision, recall and kappa against business truth, and where groundedness falls short

### Related Resources
- [Snowflake AI Observability reference](https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-observability/reference)
- [Cortex REST API](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-rest-api)
- [Langfuse LLM-as-a-Judge](https://langfuse.com/docs/evaluation/evaluation-methods/llm-as-a-judge)
- [Langfuse Score Analytics](https://langfuse.com/docs/evaluation/scores/score-analytics)
- [Langfuse docker-compose.yml](https://github.com/langfuse/langfuse/blob/main/docker-compose.yml)
