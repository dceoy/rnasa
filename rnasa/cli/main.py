#!/usr/bin/env python
"""
Gene Expression Level Calculator for RNA-seq

Usage:
    rnasa download [--debug|--info] [--cpus=<int>] [--skip-cleaning]
        [--print-subprocesses] [--genome=<ver>] [--dest-dir=<path>]
    rnasa calculate [--debug|--info] [--cpus=<int>] [--workers=<int>]
        [--skip-cleaning] [--print-subprocesses] [--seed=<int>]
        [--sort-bam] [--skip-adapter-removal] [--skip-qc] [--dest-dir=<path>]
        <ref_path_prefix> <fq_path_prefix>...
    rnasa extract [--debug|--info] [--gct] [--name-prefix=<str>]
        [--dest-dir=<path>] <search_dir_path>
    rnasa -h|--help
    rnasa --version

Commands:
    download                Download and process resource data
    calculate               Calculate TPM (transcripts per million) values
    extract                 Extract TPM values from RSEM results files

Options:
    -h, --help              Print help and exit
    --version               Print version and exit
    --debug, --info         Execute a command with debug|info messages
    --cpus=<int>            Limit CPU cores used
    --skip-cleaning         Skip incomlete file removal when a task fails
    --print-subprocesses    Print STDOUT/STDERR outputs from subprocesses
    --genome=<ver>          Specify the genome version [default: GRCh38]
                            { GRCh38, GRCh37, GRCm39, GRCm38 }
    --dest-dir=<path>       Specify a destination directory path [default: .]
    --workers=<int>         Specify the maximum number of workers [default: 1]
    --seed=<int>            Set a random seed
    --sort-bam              Sort output BAM files by coordinate
    --skip-adapter-removal  Skip adapter removal
    --skip-qc               Skip QC-checks
    --gct                   Write expression data in GCT format
    --name-prefix=<str>     Specify a output name prefix [default: samples]

Args:
    <ref_path_prefix>       Path prefix as an RSEM reference name
    <fq_path_prefix>        Path prefixes as FASTQ names
    <search_dir_path>       Path to search for RSEM results files
"""

import logging
import os
from math import floor
from pathlib import Path
from random import randint

from docopt import docopt
from ftarc.cli.util import (build_luigi_tasks, fetch_executable, print_log,
                            print_yml, read_yml, render_luigi_log_cfg)
from psutil import cpu_count, virtual_memory

from .. import __version__
from ..task.controller import PrintEnvVersions, RunRnaseqPipeline
from ..task.rsem import PrepareRsemReferenceFiles
from .tpm import extract_tpm_values


def main():
    args = docopt(__doc__, version=__version__)
    if args['--debug']:
        console_log_level = 'DEBUG'
    elif args['--info']:
        console_log_level = 'INFO'
    else:
        console_log_level = 'WARNING'
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S', level=console_log_level
    )
    logger = logging.getLogger(__name__)
    logger.debug(f'args:{os.linesep}{args}')
    print_log(f'Start the workflow of rnasa {__version__}')
    n_cpu = int(args['--cpus'] or cpu_count())
    dest_dir = Path(args['--dest-dir']).resolve()
    log_dir = (dest_dir.joinpath('log') if args['calculate'] else dest_dir)
    sh_config = {
        'log_dir_path': str(log_dir),
        'remove_if_failed': (not args['--skip-cleaning']),
        'quiet': (not args['--print-subprocesses']),
        'executable': fetch_executable('bash')
    }
    if args['download']:
        url_dict = read_yml(
            path=Path(__file__).parent.parent.joinpath('static/urls.yml')
        ).get(args['--genome'])
        assert bool(url_dict), 'unsupprted genome version'
        build_luigi_tasks(
            tasks=[
                PrepareRsemReferenceFiles(
                    fna_url=url_dict['genomic_fna'],
                    gtf_url=url_dict['genomic_gtf'],
                    dest_dir_path=args['--dest-dir'],
                    genome_version=args['--genome'],
                    **_generate_command_dict(
                        'wget', 'pigz', 'STAR', 'rsem-prepare-reference',
                        'rsem-refseq-extract-primary-assembly',
                        'rsem-calculate-expression'
                    ),
                    n_cpu=n_cpu, sh_config=sh_config
                )
            ],
            log_level=console_log_level
        )
    elif args['calculate']:
        n_sample = len(args['<fq_path_prefix>'])
        memory_mb = virtual_memory().total / 1024 / 1024 / 2
        n_worker = min(int(args['--workers']), n_cpu, n_sample)
        command_dict = _generate_command_dict(
            'pigz', 'pbzip2', 'trim_galore', 'cutadapt', 'fastqc', 'STAR',
            'rsem-calculate-expression', 'samtools',
            *(list() if args['--skip-qc'] else ['plot-bamstats', 'gnuplot'])
        )
        kwargs = {
            'ref_path_prefix': args['<ref_path_prefix>'],
            'dest_dir_path': str(dest_dir), 'sort_bam': args['--sort-bam'],
            'adapter_removal': (not args['--skip-adapter-removal']),
            'qc': (not args['--skip-qc']),
            'seed': int(args['--seed'] or randint(0, 2147483647)),
            **command_dict,
            'samtools_qc_commands': (
                list() if args['--skip-qc'] else (
                    ['flagstat', 'coverage', 'idxstats', 'stats']
                    if args['--sort-bam'] else ['flagstat', 'stats']
                )
            ),
            'n_cpu': max(floor(n_cpu / n_worker), 1),
            'memory_mb': (memory_mb / n_worker), 'sh_config': sh_config
        }
        print_log(f'Analyze gene expressions:\t{dest_dir}')
        print_yml([
            {
                'config': [
                    {'adapter_removal': kwargs['adapter_removal']},
                    {'sort_bam': kwargs['sort_bam']}, {'qc': kwargs['qc']},
                    {'seed': kwargs['seed']}, {'n_worker': n_worker},
                    {'n_cpu': kwargs['n_cpu']},
                    {'memory_mb': kwargs['memory_mb']}
                ]
            },
            {
                'input': [
                    {'n_sample': n_sample},
                    {'reference': args['<ref_path_prefix>']},
                    {'samples': args['<fq_path_prefix>']}
                ]
            }
        ])
        log_cfg_path = str(log_dir.joinpath('luigi.log.cfg'))
        render_luigi_log_cfg(
            log_cfg_path=log_cfg_path, console_log_level=console_log_level,
            file_log_level='DEBUG'
        )
        build_luigi_tasks(
            tasks=[
                PrintEnvVersions(
                    command_paths=[
                        v for k, v in command_dict.items()
                        if k != 'plot_bamstats'
                    ],
                    sh_config=sh_config
                )
            ],
            log_level=console_log_level, logging_conf_file=log_cfg_path,
            hide_summary=True
        )
        build_luigi_tasks(
            tasks=[
                RunRnaseqPipeline(fq_path_prefix=p, priority=i, **kwargs)
                for i, p in
                zip(range(n_sample * 100, 0, -100), args['<fq_path_prefix>'])
            ],
            workers=n_worker, log_level=console_log_level,
            logging_conf_file=log_cfg_path
        )
    elif args['extract']:
        extract_tpm_values(
            search_dir_path=args['<search_dir_path>'],
            dest_dir_path=str(dest_dir), gct=args['--gct'],
            name_prefix=args['--name-prefix']
        )


def _generate_command_dict(*cmds):
    return({c.replace('-', '_').lower(): fetch_executable(c) for c in cmds})
