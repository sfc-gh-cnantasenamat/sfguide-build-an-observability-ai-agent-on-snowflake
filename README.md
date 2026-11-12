# Build an Observability AI Agent on Snowflake

Build three LangGraph refund agents on Cortex, observe them with TruLens and a self-hosted Langfuse on Snowpark Container Services, and use Snowflake business data to find the false successes that cost, self-reported success and LLM judges miss.

- Guide: [`quickstart/build-an-observability-ai-agent-on-snowflake/build-an-observability-ai-agent-on-snowflake.md`](quickstart/build-an-observability-ai-agent-on-snowflake/build-an-observability-ai-agent-on-snowflake.md)
- Companion files: [build-an-observability-ai-agent-on-snowflake](https://github.com/sfc-gh-cnantasenamat/build-an-observability-ai-agent-on-snowflake), with the agents in `src/`, the data model and business-truth views in `sql/`, Langfuse on SPCS in `langfuse_spcs/`, the notebook in `notebooks/` and the Streamlit app in `refund_truth_sis/`.

> **Draft:** not yet run end-to-end in a fresh account. All data is synthetic; nothing touches a real payment system.
