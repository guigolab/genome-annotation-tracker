from typing import Optional
from array import array

class Root:
    __slots__ = ('feature_type', 'biotype', 'length', 'has_cds', 'has_exon')

    def __init__(self, feature_type: str, biotype: Optional[str], length: int):
        self.feature_type = feature_type
        self.biotype = biotype
        self.length = length
        self.has_cds = False
        self.has_exon = False

class Transcript:
    __slots__ = ('gene_id', 'type', 'exons_flat', 'exon_len_sum', 'cds_total_len', 'cds_segments', 'cds_lens')

    def __init__(self, gene_id: str, ttype: Optional[str]):
        self.gene_id = gene_id
        self.type = ttype
        self.exons_flat = array('i')
        self.exon_len_sum = 0
        self.cds_total_len = 0
        self.cds_segments = 0
        self.cds_lens = array('i')


class OrphanFeature:
    __slots__ = ('feature_id', 'feature_type', 'start', 'end', 'parent_ids', 'biotype')
    
    def __init__(self, feature_id: str, feature_type: str, start: int, end: int, parent_ids: list, biotype: Optional[str]):
        self.feature_id = feature_id
        self.feature_type = feature_type
        self.start = start
        self.end = end
        self.parent_ids = parent_ids
        self.biotype = biotype

