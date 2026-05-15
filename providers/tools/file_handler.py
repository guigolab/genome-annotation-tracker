import csv
import os


def load_annotations(file_path, key_column: str = "access_url") -> dict[str, dict]:
    """
    Load existing annotations from a file.
    Returns a dict of annotations with the accession as the key.
    """
    annotations_dict, _ = load_annotations_ordered(file_path, key_column)
    return annotations_dict


def load_annotations_ordered(
    file_path: str, key_column: str = "access_url"
) -> tuple[dict[str, dict], list[str]]:
    """
    Load annotations preserving first-seen key order (for stable git diffs).
    Returns (dict keyed by key_column, ordered list of those keys).
    """
    if not os.path.isfile(file_path):
        return {}, []
    annotations_dict: dict[str, dict] = {}
    key_order: list[str] = []
    seen: set[str] = set()
    with open(file_path, "r") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            k = row.get(key_column)
            if not k:
                continue
            annotations_dict[k] = row
            if k not in seen:
                key_order.append(k)
                seen.add(k)
    return annotations_dict, key_order


def write_annotations(annotations: list[dict], file_path: str):
    """
    Write annotations to a file.
    Row order is exactly the order of the input list (callers should pass
    git-friendly ordering: prior keys in file order, then new keys).
    """
    if not annotations:
        raise ValueError("Cannot write empty annotations list")
    with open(file_path, "w") as f:
        writer = csv.DictWriter(f, annotations[0].keys(), delimiter="\t")
        writer.writeheader()
        writer.writerows(annotations)
        