#!/usr/bin/env python
"""
RNA-seq Data Analyzer

Usage:
    rnasa download [--debug|--info] [--cpus=<int>] [--workers=<int>]
        [--skip-cleaning] [--print-subprocesses] [--genome=<ver>]
        [--dest-dir=<path>]
    rnasa run [--debug|--info] [--cpus=<int>] [--workers=<int>]
        [--skip-cleaning] [--print-subprocesses] [--seed=<int>]
        [--skip-adapter-removal] [--skip-qc] [--dest-dir=<path>]
        <ref_path_prefix> <fq_path_prefix>...
    rnasa -h|--help
    rnasa --version

Commands:
    download                Download and process resource data
    run                     Run the analytical pipeline

Options:
    -h, --help              Print help and exit
    --version               Print version and exit
    --debug, --info         Execute a command with debug|info messages
    --cpus=<int>            Limit CPU cores used
    --workers=<int>         Specify the maximum number of workers [default: 1]
    --skip-cleaning         Skip incomlete file removal when a task fails
    --print-subprocesses    Print STDOUT/STDERR outputs from subprocesses
    --genome=<ver>          Specify the genome version [default: GRCh38]
                            { GRCh38, GRCh37, GRCm39, GRCm38 }
    --dest-dir=<path>       Specify a destination directory path [default: .]
    --seed=<int>            Set a random seed
    --skip-adapter-removal  Skip adapter removal
    --skip-qc               Skip QC-checks

Args:
    <ref_path_prefix>       RSEM reference name
    <fq_path_prefix>        Path to a FASTQ file
"""

import logging
import os
from math import floor
from pathlib import Path

from docopt import docopt
from ftarc.cli.util import (build_luigi_tasks, fetch_executable, print_log,
                            read_yml)
from psutil import cpu_count, virtual_memory

from .. import __version__
from ..task.rsem import PrepareRsemReferenceFiles, RunRnaseqPipeline


def main():
    args = docopt(__doc__, version=__version__)
    if args['--debug']:
        log_level = 'DEBUG'
    elif args['--info']:
        log_level = 'INFO'
    else:
        log_level = 'WARNING'
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S', level=log_level
    )
    logger = logging.getLogger(__name__)
    logger.debug(f'args:{os.linesep}{args}')
    print_log(f'Start the workflow of rnasa {__version__}')
    n_cpu = int(args['--cpus'] or cpu_count())
    sh_config = {
        'log_dir_path': args['--dest-dir'],
        'remove_if_failed': (not args['--skip-cleaning']),
        'quiet': (not args['--print-subprocesses']),
        'executable': fetch_executable('bash')
    }
    if args['download']:
        url_dict = read_yml(
            path=Path(__file__).parent.parent.joinpath('static/urls.yml')
        ).get(args['--genome'])
        assert bool(url_dict), 'unsupprted genome version'
        command_dict = {
            c.replace('-', '_').lower(): fetch_executable(c)
            for c in ['wget', 'pigz', 'STAR', 'rsem-calculate-expression']
        }
        build_luigi_tasks(
            tasks=[
                PrepareRsemReferenceFiles(
                    fna_url=url_dict['genomic_fna'],
                    gtf_url=url_dict['genomic_gtf'],
                    dest_dir_path=args['--dest-dir'],
                    genome_version=args['--genome'],
                    **command_dict, n_cpu=n_cpu, sh_config=sh_config
                )
            ],
            log_level=log_level
        )
    elif args['run']:
        n_sample = len(args['<fq_path_prefix>'])
        memory_mb = virtual_memory().total / 1024 / 1024 / 2
        n_worker = min(int(args['--workers']), n_cpu, n_sample)
        kwargs = {
            'ref_path_prefix': args['<ref_path_prefix>'],
            'dest_dir_path': args['--dest-dir'],
            'adapter_removal': (not args['--skip-adapter-removal']),
            'qc': (not args['--skip-qc']), 'seed': args['--seed'],
            **{
                c.replace('-', '_').lower(): fetch_executable(c) for c in [
                    'pigz', 'pbzip2', 'trim_galore', 'cutadapt', 'fastqc',
                    'STAR', 'rsem-calculate-expression', 'samtools'
                ]
            },
            'samtools_qc_commands': (
                list() if args['--skip-bam-qc']
                else ['coverage', 'flagstat', 'idxstats', 'stats']
            ),
            'n_cpu': max(floor(n_cpu / n_worker), 1),
            'memory_mb': (memory_mb / n_worker),
            'sh_config': sh_config,
        }
        build_luigi_tasks(
            tasks=[
                RunRnaseqPipeline(fq_path_prefix=p, priority=i, **kwargs)
                for i, p in
                zip(range(n_sample * 100, 0, -100), args['<fq_path_prefix>'])
            ],
            workers=n_worker, log_level=log_level
        )
