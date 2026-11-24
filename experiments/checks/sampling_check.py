#!/usr/bin/env python3
"""Does the model give different output when the same request is sent several times?

Background: with thinking enabled the provider of A0 ignores `temperature` and exposes no
`seed`. The replications of a cell then differ only through the provider's own sampling.
This check shows whether that sampling exists, before any run depends on it.

It sends one identical request N times and reports
  - how many distinct answers and distinct reasoning texts came back,
  - with --logprobs, the share of answer tokens that were not the most likely token
    (above zero means the provider samples; zero on every call means greedy decoding),
  - model string, fingerprint and token counts per call.
Raw responses are stored under out/ as the evidence for whatever is reported.

Usage (from the repository root; reads LLM_API_KEY, LLM_BASE_URL, LLM_MODEL like src/llm_etl/config.py):

  .venv/bin/python experiments/checks/sampling_check.py \
      --unit experiments/sample/out/units/<a development unit>.txt --n 5 --logprobs

Other hosts of the same open weights (to compare the access path of A0):

  --seed 42                 send a seed; with a seed, identical answers mean the seed takes effect
  --effort 75               a number is sent as a number (the DeepSeek API reads high as 75,
                            the open-source serving stack reads high as 50, so send the number)
  --plain                   leave out the DeepSeek-specific fields `thinking` and `reasoning_effort`
  --extra-json '{...}'      merged into the request, for host-specific fields. Through OpenRouter,
                            pinned to one host and with thinking on:
      --base-url https://openrouter.ai/api/v1 --model deepseek/deepseek-v4.1-flash-1017 --plain \
      --extra-json '{"reasoning":{"effort":"high"},"provider":{"only":["nebius/fp8"],"allow_fallbacks":false,"require_parameters":true}}'

Cost: N calls on one source unit, a few cents at most.
"""
import argparse
import datetime
import hashlib
import json
import os
import pathlib
import sys

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

INSTRUCTION = (
    "Read the source text below. List every disposal rule, fee or obligation it states as a JSON array. "
    "Each element has the keys subject, predicate, object and qualifiers. Use only what the text says. "
    "Answer with the JSON array and nothing else.\n\n--- SOURCE ---\n"
)


def sha(text):
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()[:12]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--unit", required=True, help="text file sent as the source (a development unit)")
    ap.add_argument("--n", type=int, default=5, help="number of identical requests (default 5)")
    ap.add_argument("--effort", default="high", help="reasoning effort, a name or a number (default high)")
    ap.add_argument("--seed", type=int, default=None, help="seed to send (default: none)")
    ap.add_argument("--plain", action="store_true", help="omit the DeepSeek-specific fields thinking and reasoning_effort")
    ap.add_argument("--extra-json", default=None, help="JSON object merged into the request body")
    ap.add_argument("--no-thinking", action="store_true", help="send thinking disabled, for comparison")
    ap.add_argument("--temperature", type=float, default=0.2, help="temperature to request (default 0.2)")
    ap.add_argument("--max-tokens", type=int, default=16000)
    ap.add_argument("--logprobs", action="store_true", help="also request token log-probabilities")
    ap.add_argument("--base-url", default=os.getenv("LLM_BASE_URL") or "https://api.deepseek.com")
    ap.add_argument("--model", default=os.getenv("LLM_MODEL") or "deepseek-flash")
    ap.add_argument("--timeout", type=int, default=600)
    args = ap.parse_args()

    key = os.getenv("LLM_API_KEY")
    if not key:
        sys.exit("LLM_API_KEY is not set (environment or .env)")
    unit = pathlib.Path(args.unit).read_text(encoding="utf-8")
    body = {
        "model": args.model,
        "messages": [{"role": "user", "content": INSTRUCTION + unit}],
        "temperature": args.temperature,
        "top_p": 1.0,
        "max_tokens": args.max_tokens,
        "stream": False,
    }
    if not args.plain:
        body["thinking"] = {"type": "disabled" if args.no_thinking else "enabled"}
        if not args.no_thinking:
            body["reasoning_effort"] = int(args.effort) if args.effort.isdigit() else args.effort
    if args.seed is not None:
        body["seed"] = args.seed
    if args.extra_json:
        body.update(json.loads(args.extra_json))
    if args.logprobs:
        body.update({"logprobs": True, "top_logprobs": 2})

    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = pathlib.Path(__file__).parent / "out" / f"sampling_check_{stamp}"
    out.mkdir(parents=True, exist_ok=True)
    (out / "request.json").write_text(json.dumps(body, ensure_ascii=False, indent=2), encoding="utf-8")

    rows = []
    for i in range(1, args.n + 1):
        sent = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
        resp = requests.post(args.base_url.rstrip("/") + "/chat/completions", json=body, timeout=args.timeout,
                             headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
        (out / f"response_{i}.json").write_text(resp.text, encoding="utf-8")
        if resp.status_code != 200:
            print(f"call {i}: HTTP {resp.status_code}: {resp.text[:300]}")
            if args.logprobs and i == 1:
                print("  hint: try again without --logprobs")
            sys.exit(1)
        data = resp.json()
        choice = data["choices"][0]
        msg = choice.get("message", {})
        usage = data.get("usage", {})
        details = usage.get("completion_tokens_details") or {}
        reasoning = msg.get("reasoning_content") or msg.get("reasoning") or ""
        not_top = total = None
        tokens = ((choice.get("logprobs") or {}).get("content")) or []
        if tokens:
            total = len(tokens)
            not_top = sum(1 for t in tokens if t.get("top_logprobs") and t["token"] != max(t["top_logprobs"], key=lambda c: c["logprob"])["token"])
        rows.append({
            "call": i, "sent_utc": sent, "model": data.get("model"), "host": data.get("provider"), "fingerprint": data.get("system_fingerprint"),
            "answer_sha": sha(msg.get("content")), "answer_chars": len(msg.get("content") or ""),
            "reasoning_sha": sha(reasoning), "reasoning_chars": len(reasoning),
            "input_tokens": usage.get("prompt_tokens"), "output_tokens": usage.get("completion_tokens"),
            "reasoning_tokens": details.get("reasoning_tokens"), "finish": choice.get("finish_reason"),
            "tokens_not_top": not_top, "tokens_with_logprobs": total,
        })
        r = rows[-1]
        print(f"call {i}: answer {r['answer_sha']} ({r['answer_chars']} chars)  reasoning {r['reasoning_sha']} ({r['reasoning_chars']} chars)  "
              f"tokens in/out/reasoning {r['input_tokens']}/{r['output_tokens']}/{r['reasoning_tokens']}  finish {r['finish']}  "
              f"model {r['model']}  host {r['host']}  fingerprint {r['fingerprint']}")

    answers = len({r["answer_sha"] for r in rows})
    reasonings = len({r["reasoning_sha"] for r in rows})
    summary = {"n": args.n, "base_url": args.base_url, "request": {k: v for k, v in body.items() if k != "messages"}, "distinct_answers": answers, "distinct_reasoning_texts": reasonings,
               "calls": rows}
    print(f"\n{answers} distinct answers and {reasonings} distinct reasoning texts in {args.n} identical requests.")
    with_lp = [r for r in rows if r["tokens_with_logprobs"]]
    if with_lp:
        nt, tt = sum(r["tokens_not_top"] for r in with_lp), sum(r["tokens_with_logprobs"] for r in with_lp)
        summary["answer_tokens_not_most_likely"] = {"count": nt, "of": tt}
        print(f"{nt} of {tt} answer tokens were not the most likely token ({100 * nt / tt:.1f} %).")
    elif args.logprobs:
        print("The response carried no log-probabilities for the answer.")
    if args.seed is not None:
        print("With a seed: 1 distinct answer means the seed takes effect on this host; more than 1 means it is accepted but does not make calls repeat.")
    elif answers == 1 and reasonings == 1:
        print("READ THIS: every call returned the same text. Replications of a cell may then be near-identical; raise it before F3.")
    (out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Raw responses and summary: {out}")


if __name__ == "__main__":
    main()
