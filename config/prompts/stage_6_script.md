You are writing a script in the niche:
"AI + automations to make money (agency/workflow/monetization)".

You will receive structured fields for one idea row.

Hard rules:
- Use ONLY the provided fields. Do not invent tools, steps, metrics, outcomes, or proof.
- If metrics are referenced, phrase them as claims unless explicitly verified by evidence in the provided fields.
- Hooks must avoid clickbait and reference a concrete mechanism/tool/workflow when possible.
- Include 3 to 6 workflow bullets in plain language, and every bullet line MUST start with "- ".
- Mention relevant tools when available.
- Output MUST be valid JSON only (no extra text). Use double quotes and no trailing commas.

Format policy:
- If recommended_format is in {"shorts", "reel"}:
- script word count target: at least 170 words.
  - script estimated_seconds: 45 to 70.
- If recommended_format is in {"tweet", "thread", "linkedin", "other"}:
  - script word count target: at least 200 words.
  - script estimated_seconds: 70 to 110.

Return JSON with EXACTLY these keys and structure:
{
  "primary_hook": "string",
  "alt_hooks": ["string", "string"],
  "script": {
    "sections": [
      {"label": "hook", "text": "string"},
      {"label": "setup", "text": "string"},
      {"label": "steps", "text": "string"},
      {"label": "cta", "text": "string"}
    ],
    "word_count": 0,
    "estimated_seconds": 0
  },
  "cta": "string",
  "disclaimer": "string"
}

Section guidance:
- hook: specific and concrete, with enough detail to set context.
- setup: fuller context + why it matters for money/workflows (do not compress).
- steps: exactly 3 to 6 bullet lines, each line starts with "- " and contains actionable text with concrete detail.
- cta: direct next action for the audience, with enough specificity to be useful.

Length enforcement instructions:
- Do not optimize for brevity.
- Prefer fuller explanations over compact phrasing.
- If recommended_format is {"shorts", "reel"}, ensure total script section text is 170+ words.
- If recommended_format is {"tweet", "thread", "linkedin", "other"}, ensure total script section text is 200+ words.
- If initially below target, expand setup and steps with concrete detail before finalizing JSON.

CTA rule:
- Top-level "cta" must exactly match script.sections[label=cta].text.

Disclaimer guidance:
- If metrics_claims is non-empty, include: "Metric claims are from the source and may not generalize."
- Otherwise output an empty string.

INPUTS:
- platform: {{PLATFORM}}
- recommended_format: {{RECOMMENDED_FORMAT}}
- title: {{TITLE}}
- url: {{URL}}
- topic: {{TOPIC}}
- core_claim: {{CORE_CLAIM}}
- workflow_steps: {{WORKFLOW_STEPS}}
- tools_mentioned: {{TOOLS_MENTIONED}}
- monetization_angle: {{MONETIZATION_ANGLE}}
- metrics_claims: {{METRICS_CLAIMS}}
- assumptions: {{ASSUMPTIONS}}
- prior_hooks: {{PRIOR_HOOKS}}
