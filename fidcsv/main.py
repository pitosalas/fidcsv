#!/usr/bin/env python3

import os
import shutil
from pathlib import Path
from box import Box
import pandas as pd
from charapi import evaluate_charity

def compute_ein_aggregates(df, ein, aggregate_config):
    """Compute aggregates from input CSV based on configuration.

    Args:
        df: Input DataFrame
        ein: Employer Identification Number to aggregate
        aggregate_config: Config dict mapping field names to config dicts with:
                         - include: bool
                         - csv_field: str (column name in df)
                         - aggregation: str ('sum', 'count', 'max', 'first')
                         - data_clean: str ('currency' or 'none')

    Returns:
        Dict of field_name -> aggregated value for included fields
    """
    result = {}
    ein_data = df[df["Tax ID"] == ein]

    if ein_data.empty:
        return result

    for field_name, field_config in aggregate_config.items():
        if not field_config.get("include", False):
            continue

        csv_field = field_config.get("csv_field")
        aggregation = field_config.get("aggregation")
        data_clean = field_config.get("data_clean", "none")

        if csv_field not in ein_data.columns:
            continue

        column_data = ein_data[csv_field]

        if aggregation == "sum":
            if data_clean == "currency":
                numeric = column_data.str.replace(r"[\$,]", "", regex=True).astype(float, errors="ignore")
                result[field_name] = numeric.sum()
            else:
                result[field_name] = column_data.astype(float, errors="ignore").sum()

        elif aggregation == "count":
            result[field_name] = len(ein_data)

        elif aggregation == "max":
            if data_clean == "currency":
                numeric = column_data.str.replace(r"[\$,]", "", regex=True).astype(float, errors="ignore")
                result[field_name] = numeric.max()
            else:
                result[field_name] = pd.to_datetime(column_data, errors="coerce").max()

        elif aggregation == "first":
            result[field_name] = column_data.iloc[0] if len(column_data) > 0 else None

    return result

def extract_field_value(result, field):
    if hasattr(result, field):
        return getattr(result, field)
    if hasattr(result, "financial_metrics") and hasattr(result.financial_metrics, field):
        return getattr(result.financial_metrics, field)
    if hasattr(result, "compliance_check") and hasattr(result.compliance_check, field):
        return getattr(result.compliance_check, field)
    if hasattr(result, "external_validation") and hasattr(result.external_validation, field):
        return getattr(result.external_validation, field)
    if hasattr(result, "organization_type") and hasattr(result.organization_type, field):
        return getattr(result.organization_type, field)
    return None

def process_batch(eins, fields, charapi_config, input_df, aggregate_config, start_idx, total_eins):
    rows = []

    for idx, ein in enumerate(eins, 1):
        overall_idx = start_idx + idx
        print(f"[{overall_idx}/{total_eins}] Evaluating {ein}...")
        result = evaluate_charity(ein, charapi_config)

        row_data = {field: extract_field_value(result, field) for field in fields}
        aggregates = compute_ein_aggregates(input_df, ein, aggregate_config)
        row_data.update(aggregates)
        rows.append(row_data)

    return pd.DataFrame(rows, columns=fields)

def main():
    config_path = Path(__file__).parent / "config" / "config.yaml"
    config = Box.from_yaml(filename=config_path)

    print("fidcsv - Fidelity Charitable CSV Processor")
    print(f"Data file: {config.data}")

    output_dir = Path(config.output_dir)
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)
    print(f"Output directory: {output_dir} (reset)")

    df = pd.read_csv(config.data)

    print(f"Loaded {len(df)} rows")
    print(f"Columns: {list(df.columns)}")

    unique_eins = df["Tax ID"].dropna().unique()
    print(f"\nFound {len(unique_eins)} unique EINs")
    print(f"Sample EINs: {list(unique_eins[:5])}")

    if config.limit > 0:
        unique_eins = unique_eins[:config.limit]
        print(f"Processing first {len(unique_eins)} EINs (limit set in config)")

    included_fields = [
        field_name
        for field_name, field_config in config.fields.items()
        if field_config.include
    ]

    print(f"\nOutput will have {len(included_fields)} columns")
    print(f"Sample columns: {list(included_fields[:5])}...")

    if config.batch > 0:
        total_batches = (len(unique_eins) + config.batch - 1) // config.batch
        print(f"\nProcessing {len(unique_eins)} EINs in {total_batches} batches of {config.batch}")

        for batch_num in range(total_batches):
            start = batch_num * config.batch
            end = min(start + config.batch, len(unique_eins))
            batch_eins = unique_eins[start:end]

            print(f"\n=== Batch {batch_num + 1}/{total_batches} ===")
            batch_df = process_batch(batch_eins, included_fields, config.charapi_config_path, df, config.input_aggregates, start, len(unique_eins))

            output_filename = f"{config.output}_batch_{batch_num + 1}.csv"
            output_file = output_dir / output_filename
            batch_df.to_csv(output_file, index=False)
            print(f"Batch {batch_num + 1} written to {output_file}")
    else:
        print(f"\nEvaluating {len(unique_eins)} charities (no batching)...")
        output_df = process_batch(unique_eins, included_fields, config.charapi_config_path, df, config.input_aggregates, 0, len(unique_eins))
        output_file = output_dir / f"{config.output}.csv"
        output_df.to_csv(output_file, index=False)
        print(f"\nOutput written to {output_file}")

def sanity_check(output_dir, output_name):
    import glob
    batch_files = sorted(glob.glob(str(output_dir / f"{output_name}_batch_*.csv")))
    if not batch_files:
        print("No batch files found for sanity check")
        return

    all_data = pd.concat([pd.read_csv(f) for f in batch_files], ignore_index=True)

    distinct_charities = all_data["ein"].nunique()
    total_donated = all_data["total_donated"].sum()
    cause_dist = all_data["charitable_sector"].value_counts().sort_values(ascending=False)

    print("\n" + "="*70)
    print("SANITY CHECK REPORT")
    print("="*70)
    print(f"\nDistinct charities evaluated: {distinct_charities}")
    print(f"Total donated across all charities: ${total_donated:,.2f}")
    print("\nDistribution of charitable sectors:")
    print("-" * 70)
    for sector, count in cause_dist.items():
        pct = (count / len(all_data)) * 100
        print(f"  {sector:40s} {count:3d} charities ({pct:5.1f}%)")
    print("="*70)

if __name__ == "__main__":
    main()

    config_path = Path(__file__).parent / "config" / "config.yaml"
    config = Box.from_yaml(filename=config_path)
    output_dir = Path(config.output_dir)
    sanity_check(output_dir, config.output)
