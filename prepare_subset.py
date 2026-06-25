# scripts/prepare_subset.py
import os
import h5py
import numpy as np
import pandas as pd

N_PER_CLASS    = int(os.getenv("N_PER_CLASS", "500"))
NOISE_CSV      = os.getenv("NOISE_CSV", "data/noise.csv")
EARTHQUAKE_CSV = os.getenv("EARTHQUAKE_CSV", "data/local_earthquakes.csv")
NOISE_H5       = os.getenv("NOISE_H5", "data/noise.h5")
EARTHQUAKE_H5  = os.getenv("EARTHQUAKE_H5", "data/local_earthquakes.h5")
OUTPUT_CSV     = os.getenv("OUTPUT_CSV", "data/subset_metadata.csv")
OUTPUT_H5      = os.getenv("OUTPUT_H5", "data/subset_waveforms.h5")

RANDOM_SEED    = 42

def sample_valid_rows(csv_path: str, h5_path: str, n: int, seed: int) -> pd.DataFrame:
    """
    Campiona n righe dal CSV verificando che il waveform
    corrispondente esista nell'HDF5.
    """
    df = pd.read_csv(csv_path, low_memory=False)
    df = df.sample(frac=1, random_state=seed).reset_index(drop=True)

    valid_rows = []
    with h5py.File(h5_path, "r") as hf:
        available = set(hf["data"].keys())
        for _, row in df.iterrows():
            if len(valid_rows) >= n:
                break
            if row["trace_name"] in available:
                valid_rows.append(row)

    print(f"  Sampled {len(valid_rows)}/{n} valid rows from {csv_path}")
    return pd.DataFrame(valid_rows).reset_index(drop=True)


def copy_waveforms(src_h5: str, rows: pd.DataFrame, dst: h5py.File):
    """
    Copia i waveform selezionati dall'HDF5 sorgente
    nell'HDF5 di output sotto data/<trace_name>.
    """
    with h5py.File(src_h5, "r") as src:
        for _, row in rows.iterrows():
            trace_name = row["trace_name"]
            waveform   = np.array(src["data"][trace_name])
            if trace_name not in dst["data"]:
                dst["data"].create_dataset(trace_name, data=waveform)


def main():
    print(f"Sampling {N_PER_CLASS} noise traces...")
    noise_df = sample_valid_rows(NOISE_CSV, NOISE_H5, N_PER_CLASS, RANDOM_SEED)

    print(f"Sampling {N_PER_CLASS} earthquake traces...")
    eq_df = sample_valid_rows(EARTHQUAKE_CSV, EARTHQUAKE_H5, N_PER_CLASS, RANDOM_SEED + 1)

    # Merge e shuffle
    subset = pd.concat([noise_df, eq_df], ignore_index=True)
    subset = subset.sample(frac=1, random_state=RANDOM_SEED).reset_index(drop=True)
    subset.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved {len(subset)} rows → {OUTPUT_CSV}")

    # Crea HDF5 di output
    print(f"Copying waveforms → {OUTPUT_H5}")
    with h5py.File(OUTPUT_H5, "w") as dst:
        dst.create_group("data")
        copy_waveforms(NOISE_H5, noise_df, dst)
        copy_waveforms(EARTHQUAKE_H5, eq_df, dst)

    print("Done.")
    print(f"  Noise:      {len(noise_df)} traces")
    print(f"  Earthquake: {len(eq_df)} traces")
    print(f"  Total:      {len(subset)} traces")


if __name__ == "__main__":
    main()