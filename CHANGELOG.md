# Changelog

All notable project changes are documented in this file.

## [0.1.3] - 2026-03-27

### Changed

- Pool mining rewards are no longer credited to user spendable balances immediately when a block candidate is merely accepted by the node.
- Accepted pool blocks now create pending reward payout records first and only become spendable after canonical settlement and a configurable confirmation threshold.
- Added automatic found-block reconciliation against the node canon so orphaned or reorged pool blocks can be marked and previously finalized mining rewards can be reversed safely.
- User/API balance responses and the web dashboard now show `Immature mining` separately from spendable balance.
- The admin dashboard now splits tracked obligations into spendable tracked balance plus immature mining obligations before comparing against the pool on-chain balance.
- Historical pool reward ledger entries are now backfilled into the new payout tracking model during reconciliation so existing installs can migrate forward without a full reset.
- Historical reward reconciliation was further hardened to recover legacy mining credits directly from old `BlockReward` ledger entries, even when older rounds are no longer still marked as `Won`.

## [0.1.2] - 2026-03-23

### Changed

- Miner dashboard now shows share difficulty as an integer without decimal places.
- Miner hashrate is now shown as an integer `MH/s` value.
- Mining stats now also display pool hashrate and network hashrate as integer `GH/s` values.
- Mining stats layout was reorganized and now includes a `Last share ago` status box.
- Network hashrate estimation now averages over up to the last `60` rounds.
- The WPF admin summary now includes the pool on-chain balance and a `Pool Balance Delta` value computed as pool balance minus tracked user balances.

## [0.1.0-alpha] - 2026-03-21

### Notes

- Alpha quality intended for testing.
