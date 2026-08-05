#!/usr/bin/env python3
"""
Backfill historical GTB payouts into gtb_payout_logs table.

Historical data extracted from GTB winner log messages:
- 2026-07-16: FlipTheSwitch ($6), Legend ($3), Miller ($1)
- 2026-07-16: tremorCS ($6), FlipTheSwitch ($2), AlexMou ($1)
- 2026-07-17: Mar_Cus ($3), Urbs ($2), Rexify ($1)
- 2026-07-25: CASTLEHILL ($3), FlipTheSwitch ($2), MetaxaMan ($1)
"""

from db import backfill_gtb_payouts

# Discord ID map from user
gtb_backfill_data = [
    # 2026-07-16 first round
    (1058140474621300787, "FlipTheSwitch", 6.00, 1),
    (1442526638805094453, "Legend", 3.00, 2),
    (841258849981693982, "miller", 1.00, 3),
    # 2026-07-16 second round
    (879722303313834075, "tremorCS", 6.00, 1),
    (1058140474621300787, "FlipTheSwitch", 2.00, 2),
    (909194421512339457, "AlexMou", 1.00, 3),
    # 2026-07-17
    # Mar_Cus not in map - skipped, Urbs and Rexify included
    (693310957979435008, "urbs114", 2.00, 2),
    (879083287216259072, "Rexify", 1.00, 3),
    # 2026-07-25
    (827147850101030942, "CASTLEHILL", 3.00, 1),
    (1058140474621300787, "FlipTheSwitch", 2.00, 2),
    (315569640346484739, "MetaxaMan", 1.00, 3),
]

if __name__ == "__main__":
    print(f"Backfilling {len(gtb_backfill_data)} GTB payouts...")
    backfill_gtb_payouts(gtb_backfill_data)
    print("✅ Backfill complete!")
