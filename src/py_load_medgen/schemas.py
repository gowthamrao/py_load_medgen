# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
"""
This module defines the schemas for the MedGen RRF files.
The schemas are represented as lists of column names.
These are used by the parser to map columns in the input files to
dataclass fields in a resilient way.
"""

# From: https://www.ncbi.nlm.nih.gov/books/NBK9685/
# File: MRCONSO.RRF
MRCONSO_RRF_SCHEMA = [
    "cui",
    "lat",
    "ts",
    "lui",
    "stt",
    "sui",
    "ispref",
    "aui",
    "saui",
    "scui",
    "sdui",
    "sab",
    "tty",
    "code",
    "record_str",
    "srl",
    "suppress",
    "cvf",
]

# Note: NAMES.RRF is a custom MedGen file, not a standard UMLS RRF file.
# The header is usually present, but we define the schema for resilience.
# #CUI|name|source|suppress|
NAMES_RRF_SCHEMA = [
    "cui",
    "name",
    "source",
    "suppress",
]

# From: https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.related_concepts_file_mrrel_rrf/
# File: MRREL.RRF
MRREL_RRF_SCHEMA = [
    "cui1",
    "aui1",
    "stype1",
    "rel",
    "cui2",
    "aui2",
    "stype2",
    "rela",
    "rui",
    "srui",
    "sab",
    "sl",
    "rg",
    "dir",
    "suppress",
    "cvf",
]

# From: https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.Tf/
# File: MRSTY.RRF
MRSTY_RRF_SCHEMA = [
    "cui",
    "tui",
    "stn",
    "sty",
    "atui",
    "cvf",
]

# From: https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.simple_concept_and_atom_attribute/
# File: MRSAT.RRF
MRSAT_RRF_SCHEMA = [
    "cui",
    "lui",
    "sui",
    "metaui",
    "stype",
    "code",
    "atui",
    "satui",
    "atn",
    "sab",
    "atv",
    "suppress",
    "cvf",
]

# Note: This is a tab-delimited file, not pipe-delimited RRF.
# #CUI	SDUI	HPO_ID/OMIM_ID	MedGen_Str	Source	STY
MEDGEN_HPO_MAPPING_SCHEMA = [
    "cui",
    "sdui",
    "hpo_str",
    "medgen_str",
    "medgen_str_sab",
    "sty",
]
