# Changelog

All notable project changes are documented in this file.

## [0.2.0] - 2026-04-09

### Changed

- Account provisioning now automatically creates the custodian wallet keypair and miner binding instead of sending users through a separate miner binding page.
- The dashboard now shows the miner API token directly above the shared pool address.
- The authenticated page labels now use `Pool mining`, `Wallet`, and `Qado Pay`.
- The authenticated web UI now separates `Pool mining`, `Wallet`, and `Qado Pay` again, with wallet balance/send/address-book/transaction panels on `Wallet` and internal payments on `Qado Pay`.
- `Qado Pay` now has its own username-based address book with pool-user validation plus a payments list that only shows intra-pool transfers sent or received by the signed-in user.
- Pool deposits are now attributed directly from the sending `custodian_public_key_hex` on the user account instead of relying on the legacy verified-sender indirection.
- The account dashboard now labels pending withdrawals explicitly and shows pending pool deposits before they mature into credited deposits.
- Monetary account summary labels now include `QADO`, and `Qado Pay` now shows a dedicated summary card with available balance, daily sent/received totals, net today, and the latest payment.
- Mining stats on the account page now format miner, pool, and network hashrates with adaptive units instead of forcing coarse `MH/s` and `GH/s` displays.
- New account registration can now grant a configurable internal QADO credit when the current `Pool Balance Delta` is large enough to cover it.
- Account creation now asks users to repeat the password and checks locally that both entries match before submitting the registration request.
- Qado Pay internal transfers now use an instant-submit UX with immediate optimistic balance updates, `Sending...` to `Sent✔` button feedback, duplicate-submit protection, and rollback on error.
- The `Pool mining` overview now uses the free stats slot to show the configured pool fee as a percentage.
- Mining stats on `Pool mining` now load automatically on page open and refresh in the background while the tab is visible, instead of relying on a manual `Load miner stats` button.
- The wallet page now labels the managed public address as `Wallet address` instead of the more technical `Custodian public key`.
- The withdraw hint now uses the simpler wallet-address wording instead of the older miner-binding/custodian-key explanation.
- Miner binding no longer relies on a challenge/signature flow. The backend now binds miners directly to the user's custodian wallet public key.
- Added a simple authenticated wallet page with on-chain balance, outbound transaction sending, wallet activity, and a per-user address book.
- Added backend-managed custodian wallet keypair creation and storage for user accounts.
- Reworked the authenticated web UI to use a compact profile icon menu instead of the large session status box and moved miner binding into its own menu page.

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
