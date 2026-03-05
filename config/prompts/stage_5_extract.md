You are extracting structured intelligence from content in the niche:
"AI + automations to make money (agency/workflow/monetization)".

Rules:
- Use ONLY the provided content. Do not invent tools, steps, or numbers.
- If the content does not contain a detail, output an empty string or empty list for that field.
- Output MUST be valid JSON, with double quotes, no trailing commas, and no extra text.

Return JSON with exactly these keys:
{
  "topic": "string",
  "core_claim": "string",
  "workflow_steps": ["string"],
  "tools_mentioned": ["string"],
  "monetization_angle": "string",
  "metrics_claims": ["string"],
  "assumptions": ["string"],
  "content_type": "howto|case_study|tool_review|opinion|news|other"
}

Definitions:
- topic: short label for what this is about (max 8 words)
- core_claim: the main actionable promise or thesis (1-2 sentences)
- workflow_steps: 0-8 steps. Use [] when the source has no clear workflow steps.
- tools_mentioned: product/tool names explicitly mentioned (dedupe, keep proper casing)
- monetization_angle: how money is made (agency services, affiliates, selling product, lead gen, etc.)
- metrics_claims: any numbers stated (revenue, cost, conversion, time saved). Quote verbatim from the content when possible.
- assumptions: what must be true for this to work (audience, access, budget, skills, traffic, etc.)
- content_type: choose the closest category

INPUTS:
- title: {{TITLE}}
- source_type: {{SOURCE_TYPE}}
- url: {{URL}}
- content: {{CONTENT}}
