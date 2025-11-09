| Scenario                                        | `outcome` | Counted in W-L-T? | Notes                                                                 |
| ----------------------------------------------- | --------- | ----------------: | --------------------------------------------------------------------- |
| Regular match (Dec/Fall/TF/MD/SV/OT)            | `W` / `L` |                 ✅ | Winner determined by position relative to “over” / “def.”             |
| Tie / Draw                                      | `T`       |                 ✅ | Rare in folkstyle, but handled                                        |
| Forfeit / Medical Forfeit / Injury Default / DQ | `W` / `L` |                 ✅ | Outcome based on text order; special case “over Unknown (For.)” → `W` |
| Bye                                             | `bye`     |                 ❌ | Advancement, **not** a match result                                   |
| Exhibition                                      | `U`       |                 ❌ | Shown for completeness, excluded from record                          |
| Ambiguous / Unknown                             | `U`       |                 ❌ | Does not affect totals                                                |
