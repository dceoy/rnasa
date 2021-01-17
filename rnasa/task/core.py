#!/usr/bin/env python

import re
from pathlib import Path

from ftarc.task.core import ShellTask


class RnasaTask(ShellTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def generate_version_commands(commands):
        for c in ([commands] if isinstance(commands, str) else commands):
            n = Path(c).name
            if n == 'wget':
                yield f'{c} --version | head -1'
            else:
                yield f'{c} --version'

    @staticmethod
    def parse_fq_id(fq_path):
        fq_stem = Path(fq_path).name
        for _ in range(3):
            if fq_stem.endswith(('fq', 'fastq')):
                fq_stem = Path(fq_stem).stem
                break
            else:
                fq_stem = Path(fq_stem).stem
        return (
            re.sub(
                r'[\._](read[12]|r[12]|[12]|[a-z0-9]+_val_[12]|r[12]_[0-9]+)$',
                '', fq_stem, flags=re.IGNORECASE
            ) or fq_stem
        )
