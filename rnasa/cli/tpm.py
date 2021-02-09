#!/usr/bin/env python

import logging
import os
import re
from pathlib import Path

import pandas as pd
from ftarc.cli.util import print_log


def extract_tpm_values(search_dir_path, dest_dir_path='.'):
    print_log(f'Find RSEM gene results files:\t{search_dir_path}')
    logger = logging.getLogger(__name__)
    search_dir = Path(search_dir_path).resolve()
    input_tsv_paths = _find_file_paths_by_suffix(
        search_dir_path=str(search_dir), file_suffix='.genes.results'
    )
    logger.debug(f'input_tsv_paths: {input_tsv_paths}')
    if input_tsv_paths:
        _validate_filenames(paths=input_tsv_paths)
        df = pd.DataFrame()
        for p in input_tsv_paths:
            logger.info(f'Read a TSV file: {p}')
            df_new = _extract_tpm_from_rsem_tsv(path=p)
            logger.debug(f'df_new.shape: {df_new.shape}')
            df = (df.join(df_new, how='left') if df.size else df_new)
        logger.debug(f'df:{os.linesep}{df}')
        output_tsv = Path(dest_dir_path).resolve().joinpath(
            f'{search_dir.name}.tpm.tsv.txt'
        )
        print_log(f'Write TPM values into a file:\t{output_tsv}')
        df.to_csv(output_tsv, sep='\t')
    else:
        raise FileNotFoundError('genes results files not found')


def _find_file_paths_by_suffix(search_dir_path, file_suffix):
    found_file_paths = list()
    for root, _, files in os.walk(search_dir_path):
        for file in files:
            if file.endswith(file_suffix):
                p = str(Path(root).joinpath(file))
                print(f'- {p}', flush=True)
                found_file_paths.append(p)
    return found_file_paths


def _extract_tpm_from_rsem_tsv(path):
    return pd.read_csv(path, sep='\t').pipe(
        lambda d: d[['gene_id', 'transcript_id(s)', 'TPM']]
    ).rename(
        columns={
            'TPM': re.sub(r'\.rsem\.star$', '', Path(Path(path).stem).stem)
        }
    ).set_index(['gene_id', 'transcript_id(s)'])


def _validate_filenames(paths):
    n_uniq = len({Path(p).name for p in paths})
    assert n_uniq == len(paths), 'duplicated file names'
