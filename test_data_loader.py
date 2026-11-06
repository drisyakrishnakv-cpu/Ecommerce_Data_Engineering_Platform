
from utils.data_loader import load_all_datasets, validate_datasets

if __name__ == "__main__":
    print("Loading Gold datasets...")

    datasets = load_all_datasets()

    print("\nDATASET VALIDATION")
    print(validate_datasets(datasets).to_string())

    print("\nALL DATASETS LOADED SUCCESSFULLY!")
