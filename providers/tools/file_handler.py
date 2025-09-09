import csv

def load_annotations(file_path, key_column: str = "access_url") -> dict[str, dict]:
    """
    Load existing annotations from a file.
    Returns a dict of annotations with the accession as the key.
    """
    annotations_dict = {}
    with open(file_path, "r") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            annotations_dict[row[key_column]] = row
    return annotations_dict


def write_annotations(annotations: list[dict], file_path: str):
    """
    Write annotations to a file.
    """
    with open(file_path, "w") as f:
        writer = csv.DictWriter(f, annotations[0].keys(), delimiter="\t")
        writer.writeheader()
        writer.writerows(annotations)
        