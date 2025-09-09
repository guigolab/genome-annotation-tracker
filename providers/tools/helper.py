from datetime import datetime, timedelta


def keep_recent_annotations(existing_annotations_dict: dict, parsed_annotations_dict: dict) -> list[str]:
    """
    This function keeps the recent annotations imported in the last month.
    """
    annotations_to_keep = []
    for unique_identifier, existing_annotation in existing_annotations_dict.items():
        parsed_annotation = parsed_annotations_dict.get(unique_identifier)
        if not parsed_annotation:
            continue
        existing_date = datetime.strptime(existing_annotation.get("retrieval_date"), '%Y-%m-%d').date()
        one_month_ago = datetime.now().date() - timedelta(days=30)
        if existing_date > one_month_ago:
            annotations_to_keep.append(unique_identifier)
    return annotations_to_keep


def get_tuples_to_check(annotations_to_keep: list[str], parsed_annotations_dict: dict) -> list[tuple[str, str]]:
    """
    This function returns a list of tuples (access_url, accession) to check.
    """
    tuples = []
    for unique_identifier, _ in parsed_annotations_dict.items():
        if unique_identifier not in annotations_to_keep:
            tuples.append((parsed_annotations_dict[unique_identifier]["access_url"], unique_identifier))
    
    return tuples

def handle_last_modified_date(existing_annotations_dict: dict, parsed_annotations_dict: dict, last_modified_dates: list[tuple[str, str]]) -> list[str]:
    """
    This function handles the last modified date.
    """
    annotations_to_keep = []
    for accession, last_modified_date in last_modified_dates:
        if not last_modified_date:
            if accession in existing_annotations_dict:
                annotations_to_keep.append(accession)
            continue 
        existing_annotation = existing_annotations_dict.get(accession)
        if existing_annotation and last_modified_date == existing_annotation.get("last_modified_date"):
            #unchanged, update retrieval date
            existing_annotation["retrieval_date"] = parsed_annotations_dict[accession]["retrieval_date"]
            annotations_to_keep.append(accession)
            
        else:
            # add or update the last modified date
            parsed_annotations_dict[accession]["last_modified_date"] = last_modified_date
    return annotations_to_keep


def handle_md5_checksum(existing_annotations_dict: dict, parsed_annotations_dict: dict, md5_checksums_tuples: list[tuple[str, str]]) -> list[str]:
    """
    This function handles the md5 checksum.
    """
    annotations_to_keep = []
    for unique_identifier, md5_checksum in md5_checksums_tuples:
        if not md5_checksum:
            if unique_identifier in existing_annotations_dict:
                #keep it as it is, retry in the next job
                annotations_to_keep.append(unique_identifier)
            continue
        parsed_annotations_dict[unique_identifier]["md5_checksum"] = md5_checksum
    return annotations_to_keep

def merge_annotations(existing_annotations_dict: dict, parsed_annotations_dict: dict, annotations_to_keep: list[str]) -> list[dict]:
    """
    This function merges the existing annotations and the parsed annotations.
    """
    merged_annotations = []
    for unique_identifier in annotations_to_keep:
        merged_annotations.append(existing_annotations_dict[unique_identifier])

    for unique_identifier, parsed_annotation in parsed_annotations_dict.items():
        if unique_identifier not in annotations_to_keep and parsed_annotation.get("md5_checksum") and parsed_annotation.get("last_modified_date"):
            merged_annotations.append(parsed_annotation)
    return merged_annotations