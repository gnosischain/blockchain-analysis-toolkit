#!/usr/bin/env python3
import argparse
import json
import sys

try:
    import requests
except ImportError:
    sys.stderr.write("ERROR: This script requires the requests library. Run pip install requests and try again.\n")
    sys.exit(1)


def fetch_contract_data(chain: str, address: str) -> dict:
    """
    Fetch the JSON response from Blockscout’s /api/v2/smart-contracts/{address} endpoint.
    Returns the parsed JSON object (a dict). Raises HTTPError if the request fails.
    """
    url = f"https://{chain}.blockscout.com/api/v2/smart-contracts/{address.lower()}"
    print(f"[INFO] Fetching contract data from: {url}")
    resp = requests.get(url)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        print(f"[ERROR] HTTP {resp.status_code} when fetching {address} on {chain}. URL: {url}")
        raise
    print(f"[INFO] Successfully fetched data for {address} on {chain}.")
    return resp.json()


def extract_abi_and_impls(raw: dict) -> (list, list):
    """
    Given Blockscout’s JSON, return:
      - primary_abi (as a Python list)
      - impl_addresses (a list of implementation addresses, if any)
    """
    # 1) Extract primary ABI
    abi_field = None
    if "abi" in raw:
        abi_field = raw["abi"]
    elif "abi_json" in raw:
        abi_field = raw["abi_json"]
    else:
        abi_field = []

    if isinstance(abi_field, str):
        try:
            primary_abi = json.loads(abi_field)
            print("[INFO] Parsed primary ABI (string → JSON list).")
        except json.JSONDecodeError:
            print("[WARNING] Failed to JSON-decode the 'abi' field; using empty list.")
            primary_abi = []
    elif isinstance(abi_field, list):
        primary_abi = abi_field
        print("[INFO] Primary ABI is already a list.")
    else:
        primary_abi = []
        print("[WARNING] Unexpected type for ABI field; using empty list.")

    # 2) Extract implementations array
    impl_raw = raw.get("implementations", []) or []
    impl_addresses = []

    for entry in impl_raw:
        if isinstance(entry, dict):
            if "address" in entry:
                impl_addresses.append(entry["address"])
            elif "implementation_address" in entry:
                impl_addresses.append(entry["implementation_address"])
            else:
                print(f"[WARNING] Found implementation entry without recognized address field: {entry}")
        elif isinstance(entry, str):
            impl_addresses.append(entry)
        else:
            print(f"[WARNING] Skipping unrecognized implementation entry: {entry}")

    return primary_abi, impl_addresses


def fetch_and_merge_abis(chain: str, primary_address: str) -> list:
    """
    1. Fetch primary contract’s ABI.
    2. If there are implementations, fetch each one’s ABI.
    3. Merge all ABIs into one list and return it.
    """
    print(f"[INFO] Starting to fetch primary ABI for contract {primary_address} on chain '{chain}'.")
    data = fetch_contract_data(chain, primary_address)

    # extract and log
    primary_abi, impl_addresses = extract_abi_and_impls(data)
    print(f"[INFO] Primary contract ABI entries: {len(primary_abi)} items.")
    if impl_addresses:
        print(f"[INFO] Found {len(impl_addresses)} implementation(s): {impl_addresses}")
    else:
        print("[INFO] No implementations found for this contract.")

    merged_abi = list(primary_abi)  # copy primary ABI

    # For each implementation, attempt to fetch and merge
    for idx, impl_addr in enumerate(impl_addresses, start=1):
        print(f"[INFO] ({idx}/{len(impl_addresses)}) Fetching implementation ABI for {impl_addr} ...")
        try:
            impl_data = fetch_contract_data(chain, impl_addr)
        except Exception as e:
            print(f"[ERROR] Failed to fetch implementation {impl_addr}: {e}")
            continue

        impl_abi, _ = extract_abi_and_impls(impl_data)
        print(f"[INFO] Retrieved {len(impl_abi)} ABI entries from implementation {impl_addr}.")
        merged_abi.extend(impl_abi)

    print(f"[INFO] Merged total ABI entries before deduplication: {len(merged_abi)} items.")
    return merged_abi


def dedupe_abi_entries(abi_list: list) -> list:
    """
    Remove duplicate ABI entries (deep-equal). 
    We serialize each entry to a JSON string with sorted keys, and only keep the first occurrence.
    """
    unique = []
    seen: set = set()
    for entry in abi_list:
        try:
            entry_str = json.dumps(entry, sort_keys=True)
        except (TypeError, ValueError):
            # If an entry is not serializable for some reason, skip deduplication for it.
            entry_str = None

        if entry_str is None:
            # Just append unsorted/unserializable entries directly, though that case should be rare.
            unique.append(entry)
        else:
            if entry_str not in seen:
                seen.add(entry_str)
                unique.append(entry)
    return unique


def main():
    parser = argparse.ArgumentParser(
        description="Fetch a contract’s ABI (and any proxy implementations) from Blockscout, "
                    "dedupe repeated entries, and write a combined 'abi.json' file."
    )
    parser.add_argument(
        "--chain",
        "-c",
        required=True,
        help="The chain subdomain to use (e.g. 'gnosis', 'eth', 'optimism', etc.). "
             "This will be inserted into {chain}.blockscout.com."
    )
    parser.add_argument(
        "--address",
        "-a",
        required=True,
        help="The 0x‐address of the contract you want to fetch."
    )

    args = parser.parse_args()
    chain = args.chain.strip()
    address = args.address.strip()

    try:
        merged_abi = fetch_and_merge_abis(chain, address)
    except requests.HTTPError as http_err:
        print(f"[FATAL] HTTP error while fetching contract data: {http_err}")
        sys.exit(1)
    except Exception as e:
        print(f"[FATAL] Unexpected error while fetching ABI: {e}")
        sys.exit(1)

    # Deduplicate ABI entries
    unique_abi = dedupe_abi_entries(merged_abi)
    print(f"[INFO] ABI entries after deduplication: {len(unique_abi)} items (removed {len(merged_abi) - len(unique_abi)} duplicates).")

    out_filename = "abi.json"
    try:
        with open(out_filename, "w") as fout:
            json.dump(unique_abi, fout, indent=2)
        print(f"[SUCCESS] Combined & deduped ABI written to '{out_filename}'.")
    except Exception as e:
        print(f"[ERROR] Could not write to '{out_filename}': {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
