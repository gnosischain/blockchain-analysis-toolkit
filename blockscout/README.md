
# fetch-abi

A simple Python script to fetch a smart contract’s ABI (and any proxy implementations) from Blockscout, remove duplicate entries, and save the combined result as `abi.json`.

---

## Features

- Fetches the primary contract ABI from `https://{chain}.blockscout.com/api/v2/smart-contracts/{address}`  
- Detects and fetches any proxy implementations listed under `"implementations"`  
- Merges all ABI entries (primary + implementations) into one list  
- Deduplicates identical entries before writing  
- Logs each step and implementation status to the terminal

---

## Requirements

- Python 3.7+  
- [requests](https://pypi.org/project/requests/) library  

Install dependencies:

```bash
pip install -r requirements.txt
````

---

## Usage

```bash
python fetch_abi.py --chain <chain> --address <contract_address>
```

* `--chain` (or `-c`): Blockscout subdomain (e.g. `gnosis`, `eth`, `optimism`)
* `--address` (or `-a`): 0x-address of the contract

After running, you’ll get a file named `abi.json` in the current directory.

---

## Example

```bash
python fetch_abi.py \
  --chain gnosis \
  --address 0xdDAfbb505ad214D7b80b1f830fcCc89B60fb7A83
```

Terminal output might look like:

```
[INFO] Fetching contract data from: https://gnosis.blockscout.com/api/v2/smart-contracts/0xddafb...
[INFO] Successfully fetched data for 0xdDAfbb... on gnosis.
[INFO] Primary contract ABI entries: 24 items.
[INFO] Found 5 implementation(s): ['0xAa...', '0xBb...', …]
[INFO] (1/5) Fetching implementation ABI for 0xAa... …
[INFO] Retrieved 12 ABI entries from implementation 0xAa...
…
[INFO] Merged total ABI entries before deduplication: 96 items.
[INFO] ABI entries after deduplication: 58 items (removed 38 duplicates).
[SUCCESS] Combined & deduped ABI written to 'abi.json'.
```

---

## How It Works

1. **Fetch Primary ABI**
   Queries Blockscout’s REST API for the given contract address on the specified chain.
2. **Extract Implementations**
   Parses the JSON response and looks for an `"implementations"` array.
3. **Fetch Each Implementation**
   For every implementation address, requests its ABI and appends all entries.
4. **Deduplication**
   Serializes each ABI entry as JSON (sorted keys) and filters out duplicates.
5. **Write Output**
   Saves the final list of unique ABI entries to `abi.json`.

---