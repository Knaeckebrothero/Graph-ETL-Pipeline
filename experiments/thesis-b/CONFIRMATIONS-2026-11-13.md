# Thesis B's written confirmations for the signature of Draft 1i (13 Nov 2026)

Answers to the six places marked [to confirm 13 Nov] in the agreement, plus three corrections Thesis B asks for before signing. Everything else in Draft 1i matches Thesis B's reply of 9 November and the meeting of 12 November.

## The six places

| # | Section | Item | Thesis B |
|---|---|---|---|
| 1 | 4.3 | Schema file `shared-minimum-log/1` and its five departures | **Yes to all five** (tokens once per model call; no euro field; no correctness on `cq_attempt`; `unit_id` beside `source_id`; `K2`/`K3` as anchor codes). One addition requested, see correction B below. `kgc-trace/1` is realigned to carry the shared fields under these names (version 1.1, pull request by 15 Nov); Thesis B's records `tool_call`, `item`, `reconciliation`, `intake`, `gap_trigger` stay extensions, as the README allows. |
| 2 | 4.3 | Snapshot rule for models served under an alias | **Yes.** Alias as sent, served version from the provider's documentation, published weights labelled as published, returned model string and `system_fingerprint` per call, daily provider snapshot. Thesis B applies the same rule to `gpt-6.1-sol`. |
| 3 | 4.5 | Effort level of A0's thinking mode | **Yes, high.** A1 to A10 copy the setting: A1 sends reasoning effort `high` on the Responses API (temperature and top-p are not settable for `gpt-6.1-sol` and are recorded as `not_exposed`); A2 (Qwen 3.8 27B) runs with thinking enabled where the serving stack exposes it, recorded per call. |
| 4 | 4.5 | Three numbers of A0 fixed on 16 Nov (max output tokens, per-unit budget, query timeout) | **Yes.** A10 (half budget) is defined relative to the per-unit budget fixed then. |
| 5 | 4.6 | F2 in two steps: development and pilot buckets hashed by 14 Nov, full manifest by 18 Nov | **Yes.** Thesis B reads only the pilot units before 18 Nov. |
| 6 | 4.7 | Off-peak rule on Thesis A's DeepSeek key | **Yes**, for Thesis B's cells on that key alike (A6 in the pilot and in rounds 1 to 3). Request: the "behind schedule" threshold of 21 Nov is written so that the guaranteed builds of rounds 1 to 3 (Thesis A's 8 and Thesis B's 12 per round) finish by 1 Jan under the rule. |

## Three corrections before signing

**A. Section 4.5, A0 fallback.** Thesis B's reply and configuration files named Claude Sonnet 5.5 as the fallback for A0. Since 3.3 names Claude Sonnet 5.5 as the drafting family that no run agent may use, that fallback is withdrawn. A0 has no fallback model; if the pilot shows weak tool calling, both authors decide on 21 Nov. Thesis B's files are corrected.

**B. Sections 4.2 and 4.3, `item_id`.** The `attempt_id` scheme (run id, step index, attempt index) and the `insert_attempt` record carry no identifier of the fact candidate being written. Without it, neither thesis can compute "retries until acceptance" per item, which point 12 lists as a shared field, and Thesis B's configurations A4 (zero self-corrections) and A5 (one) cannot be compared with A0 at the item level. Request: add `item_id` to `insert_attempt` in the shared minimum, stable across the self-corrections of the same candidate, `null` allowed for a harness without an item concept; the SRW harness sets it. Thesis A accepted this in the call of 13 Nov; this row puts it into the text.

**C. Section 4.7, cost (F.4).** Draft 1i narrows F.4 to Thesis B's cells on A0's model (A6, A4, A5, A7 to A10). Thesis B accepts: A1 (GPT-6.1 Sol) and A3 (Gemini 4, best effort) run on Thesis B's own keys and cost, A2 (Qwen) on Thesis A's local GPUs as listed in the build queue. Thesis B's own cost field remains a monitoring lower bound.

## Also noted

- Draft 1i v2 (13 Nov, 25 pages): the only change against the first PDF is the local anchor, Gemma 4 26B A4B (`google/gemma-4-26B-A4B-it`, L40S GPUs) instead of Gemma 4 31B. Consequence for Thesis B: A2 is Qwen3.8-27B without the Gemma alternative; no collision with either anchor.

- Pull requests for Thesis B's instrumentation are due Sunday 15 Nov (4.7, F.2); otherwise the pilot runs on the shared minimum and the extensions follow before F4.
- A9 (sub-chunk delivery) is dropped at F3 if SRW cannot deliver sub-chunks inside one task per unit (4.1). Thesis A states which.
- The Eval 2 package ("Technique v0.1", 12 Nov) is due to Thesis B by 13 Nov (3.3); Thesis B reads it 14 or 15 Nov.
- Thesis B sends the signed agreement to Prof. Fink and Prof. Bremm with the F3 result on 13 Nov (section 6).
