from typing import Optional
from .classes import Root, Transcript, OrphanFeature
import sys


def init_orphan_feature(feature_id: str, feature_type: str, start: int, end: int, parent_ids: list[str], biotype: Optional[str]) -> OrphanFeature:
    return OrphanFeature(feature_id, feature_type, start, end, parent_ids, biotype)

def map_annotations_to_process(annotations: list[dict]) -> tuple[str, str]:
    """
    Map the annotations to the process
    return a tuple of (annotation_id, original_gff_url)
    """
    return tuple((annotation.get("annotation_id"), annotation.get("source_file_info", {}).get("url_path")) for annotation in annotations)

def categorize_roots(roots: dict) -> dict:
    root_to_category = {}
    for root_id, info in roots.items():
        feature_type = info.feature_type
        biotype = info.biotype or ''
        if feature_type == 'pseudogene':
            root_to_category[root_id] = 'pseudogene'
        elif info.has_cds or 'protein_coding' in biotype.lower():
            root_to_category[root_id] = 'coding'
        elif info.has_exon:
            root_to_category[root_id] = 'non_coding'
        else:
            root_to_category[root_id] = None
    return root_to_category


def init_category_totals() -> dict:
    return {
        'coding': {'exons': 0, 'introns': 0, 'cds': 0, 'exon_len_sum': 0, 'intron_len_sum': 0, 'cds_len_sum': 0, 'cds_transcripts': 0},
        'non_coding': {'exons': 0, 'introns': 0, 'cds': 0, 'exon_len_sum': 0, 'intron_len_sum': 0, 'cds_len_sum': 0, 'cds_transcripts': 0},
        'pseudogene': {'exons': 0, 'introns': 0, 'cds': 0, 'exon_len_sum': 0, 'intron_len_sum': 0, 'cds_len_sum': 0, 'cds_transcripts': 0},
    }

def parse_gff_line_fast(cols: list) -> tuple:
    """Optimized: direct find() for specific attributes - 6-8 find() calls per line"""
    feature_type = sys.intern(cols[2])
    start = int(cols[3])
    end = int(cols[4])
    
    attr_str = cols[8]
    parent_ids = []
    biotype = None
    feature_id = None
    for attr in attr_str.split(';'):
        key, value = attr.split('=')
        if key == 'ID':
            feature_id = value
        elif key == 'Parent':
            parent_ids = value.split(',') if ',' in value else [value]
        elif key == 'biotype' or key == 'gene_biotype' or key == 'transcript_biotype':
            biotype = sys.intern(value)

    return feature_type, start, end, parent_ids, biotype, feature_id

def process_feature(feature_id:str, feature_type:str, start:int, end:int, length:int, parent_ids:list[str], biotype:Optional[str], roots:dict, id_to_root:dict, transcripts:dict):
    """Optimized with reduced redundant checks"""
    # Root feature
    if not parent_ids:
        if feature_id:
            roots[feature_id] = Root(feature_type, biotype, length)
            id_to_root[feature_id] = feature_id
        return True

    # Cache commonly used checks
    is_exon = feature_type == 'exon'
    is_cds = feature_type == 'CDS'
    get_transcript = transcripts.get
    get_root = id_to_root.get
    processed = False
    for parent_id in parent_ids:
        root_id = get_root(parent_id)
        if not root_id:
            continue
            
        if parent_id == root_id:
            # Direct child of root
            if feature_id:
                id_to_root[feature_id] = root_id
                if not is_exon and not is_cds:
                    t = get_transcript(feature_id)
                    if t is None:
                        transcripts[feature_id] = Transcript(root_id, feature_type)
                    else:
                        t.gene_id = root_id
                        t.type = feature_type
            processed = True
        else:
            # Descendant of root
            if feature_id:
                id_to_root[feature_id] = root_id
            
            if is_exon:
                t = get_transcript(parent_id)
                if t is None:
                    t = transcripts[parent_id] = Transcript(root_id, None)
                t.exons_flat.append(start)
                t.exons_flat.append(end)
                t.exon_len_sum += length
                roots[root_id].has_exon = True
            elif is_cds:
                t = get_transcript(parent_id)
                if t is None:
                    t = transcripts[parent_id] = Transcript(root_id, None)
                t.cds_total_len += length
                t.cds_segments += 1
                t.cds_lens.append(length)
                roots[root_id].has_cds = True
            
            processed = True
    
    return processed


def resolve_orphans(orphans: list[OrphanFeature], roots:dict, id_to_root:dict, transcripts:dict):
    """Optimized orphan resolution"""
    still_orphaned = []
    
    for orphan in orphans:
        length = orphan.end - orphan.start + 1
        processed = process_feature(
            orphan.feature_id,
            orphan.feature_type,
            orphan.start,
            orphan.end,
            length,
            orphan.parent_ids,
            orphan.biotype,
            roots,
            id_to_root,
            transcripts
        )
        
        if not processed:
            still_orphaned.append(orphan)
    
    return still_orphaned

