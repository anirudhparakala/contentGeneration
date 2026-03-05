You are scoring and packaging a content idea for the niche:
"AI + automations to make money (agency/workflow/monetization)".

Rules:
- Use ONLY the provided extracted fields and evidence snippets.
- Do not invent proof. If proof is missing, state that in rationale.
- Output MUST be valid JSON, no extra text.
- Return exactly 3 hooks.
- Each hook must be non-empty after trim and <= 140 chars.
- Output platform MUST equal platform_hint exactly.

Return JSON with exactly these keys:
{
  "viral_rating": 1,
  "rating_rationale": "string",
  "hooks": ["string", "string", "string"],
  "platform": "youtube|newsletter",
  "recommended_format": "shorts|tweet|linkedin|reel|thread|other"
}

Rating rubric (1-10):
- +2 if steps are concrete and replicable
- +2 if monetization is explicit and plausible
- +2 if tools stack is clear and specific
- +2 if there are proof signals (real numbers, demo, case study)
- +1 if it has a strong contrarian angle or novelty
- +1 if it is clearly relevant to monetization/agency/workflows
- If content is generic, hype-driven, or lacks operational detail, score <= 4.

INPUTS:
- platform_hint: {{PLATFORM_HINT}}   # "youtube" or "newsletter"
- title: {{TITLE}}
- topic: {{TOPIC}}
- core_claim: {{CORE_CLAIM}}
- workflow_steps: {{WORKFLOW_STEPS}}
- tools_mentioned: {{TOOLS_MENTIONED}}
- monetization_angle: {{MONETIZATION_ANGLE}}
- metrics_claims: {{METRICS_CLAIMS}}
- assumptions: {{ASSUMPTIONS}}
- content_type: {{CONTENT_TYPE}}
- evidence_snippets: {{EVIDENCE_SNIPPETS}}
