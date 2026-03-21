# Qado Pool Stack

Qado Pool Stack is a Windows WPF desktop application that hosts a Qado mining pool backend and a simple browser dashboard for pool users.

The project combines:

- a WPF admin shell for pool operations and settings
- an embedded ASP.NET Core server for the web dashboard and pool API
- SQLite persistence for users, balances, shares, rounds, deposits, ledger history, and withdrawals
- Qado node integration for mining jobs, share submission, incoming deposit polling, and transaction broadcast

The current UI intentionally uses the default WPF look and a minimal web dashboard without fancy theming.

## Current Behavior

- The desktop app is single-instance. Launching the EXE again activates the already running window instead of opening a second full instance.
- The pool uses one shared on-chain deposit address: the configured pool mining public key.
- Each user verifies exactly one miner public key through a signed challenge.
- The verified miner binding key is also used as:
  - the sender identity for deposit matching
  - the stored withdrawal address
- Deposits are credited when the pool sees an incoming on-chain transfer to the pool address and matches the transaction sender to a verified user key.
- Withdrawals are created and broadcast by the pool through the connected Qado node.
- The withdrawal fee is entered by the user, defaults to `0`, and is deducted from the requested withdrawal amount.
- The dashboard transaction history shows deposits, withdrawals, mining rewards, and internal user-to-user payments.

## Requirements

- Windows
- .NET 10 SDK for local development
- a reachable Qado node with the API endpoints used by this pool

Target framework:

```text
net10.0-windows7.0
```

## Expected Qado Node Endpoints

The pool expects the connected node to provide these endpoints:

- `POST /v1/mining/job`
- `POST /v1/mining/submit`
- `GET /v1/address/{address}`
- `GET /v1/address/{address}/incoming`
- `GET /v1/tip`
- `GET /v1/network`
- `POST /v1/tx/broadcast`

## Documentation

- `README.md` is the primary operator and developer reference.
- `CHANGELOG.md` tracks notable behavior and implementation changes over time.

## Running From Source

Build:

```powershell
dotnet build .\QadoPoolStack.slnx
```

Test:

```powershell
dotnet test .\QadoPoolStack.slnx
```

Run:

```powershell
dotnet run --project .\src\QadoPoolStack.Desktop\QadoPoolStack.Desktop.csproj
```

At startup the app creates its local runtime files under the executable directory:

- `data\poolsettings.json`
- `data\pool.db`
- `data\pool.log`
- `data\certs\`

## Web Routes

The embedded web server serves three HTML entry points:

- `/` -> login page only
- `/register` -> registration page only
- `/dashboard` -> authenticated user dashboard

The dashboard currently includes:

- `Account`
- `Withdraw`
- `Miner binding`
- `Payment`
- `Transactions`
- `Miner stats`

## API Overview

### Public auth API

These endpoints do not require prior authentication. They create a user account or return a session token:

- `POST /user/register`
- `POST /user/login`

### Session-authenticated user API

These endpoints require the session token returned by login or register:

- `GET /user/me`
- `GET /deposit`
- `POST /deposit`
- `POST /withdraw`
- `POST /ledger/transfer`
- `GET /ledger/history`

The session token is sent as:

```http
Authorization: Bearer <session-token>
```

### Session-authenticated miner binding API

These endpoints require a valid user session because they bind a miner key to an account:

- `POST /auth/challenge`
- `POST /auth/verify`

### Miner-authenticated mining API

These endpoints require a verified miner API token:

- `GET /mining/job`
- `POST /mining/submit-share`
- `GET /miner/stats`

The miner token can be sent as either:

```http
Authorization: Bearer <miner-token>
```

or:

```http
X-Miner-Token: <miner-token>
```

### Operations API

- `GET /health`

## Rate Limiting

The embedded server applies fixed-window per-minute rate limiting in four buckets:

- public auth endpoints: IP-based
- session-authenticated user endpoints: session token based with IP fallback
- miner endpoints such as `job` and `stats`: miner token based with IP fallback
- share submission: miner token based with IP fallback

Default settings are:

- `AuthRateLimitPerMinute = 20`
- `UserApiRateLimitPerMinute = 120`
- `MinerRequestRateLimitPerMinute = 120`
- `ShareRateLimitPerMinute = 240`

## Main Functional Flows

### Account registration and login

- A user registers with username and password.
- The pool creates a session token.
- The dashboard reads current account state from `GET /user/me`.

### Miner binding

- The user submits a miner public key.
- The pool creates a signed challenge.
- The user signs the exact challenge text outside the browser with the corresponding Ed25519 private key.
- The pool verifies the signature and stores the miner record.

The verified miner key is then used for:

- mining authentication
- deposit sender matching
- the stored withdrawal address shown in the dashboard

### Deposits

- Users send funds to the shared pool address.
- The pool polls the connected node through `GET /v1/address/{address}/incoming`.
- Incoming events are matched to users by sender public key.
- After the required confirmations, the matching deposit is credited into the internal ledger and user balance.

### Mining

- The pool requests node jobs with the configured pool mining public key.
- The miner receives a pool job with a node-provided `header_hex_zero_nonce`.
- The miner inserts timestamp and nonce into the header template and hashes it with BLAKE3.
- Accepted shares are stored in SQLite.
- Round accounting credits proportional mining rewards to users.

Difficulty is variable per verified miner and currently targets roughly `5` to `10` seconds per accepted share.

### Internal payments

- Users can transfer internal balance to another username.
- Optional notes are stored in ledger metadata.
- Sent and received payments appear in the transaction history.

### Withdrawals

- Users can only withdraw after a miner key has been verified.
- Withdrawals always go to the verified miner binding address.
- The user enters:
  - amount
  - fee
- The fee defaults to `0`.
- The fee is deducted from the requested amount.
- The pool builds and signs the raw transaction with the configured pool private key and broadcasts it through the connected Qado node.
- Successful withdrawals are written to the ledger history together with the broadcast `txid`.

## Transaction History

`GET /ledger/history` drives the dashboard transaction list.

The history currently includes:

- `Deposit`
- `Mining reward`
- `Payment sent`
- `Payment received`
- `Withdrawal`
- `Withdrawal reversed`
- manual credit or debit adjustments when present

The web UI sorts entries newest first and uses:

- red negative amounts for outgoing entries
- green positive amounts for incoming entries

## Configuration

Runtime settings are stored in `data\poolsettings.json`.

Important fields include:

- `NodeBaseUrl`
- `HttpPort`
- `HttpsPort`
- `EnableHttps`
- `PreferHttpsWhenAvailable`
- `AuthRateLimitPerMinute`
- `UserApiRateLimitPerMinute`
- `MinerRequestRateLimitPerMinute`
- `ShareRateLimitPerMinute`
- `DomainName`
- `LetsEncryptEmail`
- `UseLetsEncryptStaging`
- `PoolMinerPublicKey`
- `ProtectedPoolMinerPrivateKey`
- `DefaultShareDifficulty`
- `ShareTargetSecondsMin`
- `ShareTargetSecondsMax`
- `ShareJobLifetimeSeconds`
- `ChallengeLifetimeSeconds`
- `SessionLifetimeHours`
- `PoolFeeBasisPoints`
- `AddressPollSeconds`
- `DepositMinConfirmations`

Notes:

- `PoolFeeBasisPoints` uses basis points. `100` means `1.00%`, `0` means no pool fee.
- `PreferHttpsWhenAvailable` only affects which URL the desktop app prefers in status and logs when HTTPS is available. HTTP remains reachable as a fallback.
- Secrets such as the pool private key and certificate password are stored in protected form via the local secret protector.

## Persistence

The application uses SQLite. The database file is:

```text
data\pool.db
```

Current schema tables:

- `users`
- `sessions`
- `miners`
- `challenges`
- `deposit_sender_challenges`
- `verified_deposit_senders`
- `deposit_sync_state`
- `incoming_deposit_events`
- `rounds`
- `jobs`
- `shares`
- `found_blocks`
- `balances`
- `ledger_entries`
- `withdrawal_requests`

Notes:

- Some older deposit-related tables remain in the schema for compatibility, even though the current dashboard flow now centers on the verified miner binding key.
- The `users` table still contains older per-user deposit key columns, but the current operational deposit flow uses the shared pool address plus sender matching.

## Release Builds

Use the repository release script to build self-contained single-file binaries:

```powershell
powershell -ExecutionPolicy Bypass -File .\release.ps1 -Clean
```

Default behavior:

- publishes `win-x64`
- produces self-contained single-file output
- writes release files to `.\release\win-x64`
- writes checksums to `.\release\SHA256SUMS.txt`

You can override runtimes and output folder through the script parameters.

## Notes For Operators

- If the desktop window is closed, the application performs a managed shutdown of the hosted services.
- If the EXE is started again while already running, the existing window is brought to the foreground.
- The web pages are served with `no-store` cache headers so dashboard updates are not hidden behind stale HTML or JS.
