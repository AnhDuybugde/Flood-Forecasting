"""
Inspect one NPZ from Data_Training_Soft_NPZ and print keys, shapes, dtypes.
Usage: python scripts/inspect_npz.py [path]
Default path: data/2020-2025/Data_Training_Soft_NPZ/Sample_2020-01-03.npz
"""
import sys
import numpy as np

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "data/2020-2025/Data_Training_Soft_NPZ/Sample_2020-01-03.npz"
    data = np.load(path)
    print("Keys:", list(data.keys()))
    for k in data.keys():
        arr = data[k]
        print(f"  {k}: shape={arr.shape}, dtype={arr.dtype}")
    # Document: x = 8 bands (H,W), y = label (H,W)
    print("\nNPZ format: x = (8, H, W) float32 (8 layers), y = (H, W) float32 (labels)")

if __name__ == "__main__":
    main()
