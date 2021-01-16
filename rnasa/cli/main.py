#!/usr/bin/env python
"""
RNA-seq Data Analyzer

Usage:
    rnasa download [--debug|--info] [--cpus=<int>] [--workers=<int>]
        [--skip-cleaning] [--print-subprocesses] [--genome=<ver>]
        [--dest-dir=<path>]
    rnasa run [--debug|--info] [--cpus=<int>] [--workers=<int>]
        [--skip-cleaning] [--print-subprocesses] [--seed=<int>]
        [--dest-dir=<path>] <ref_path_prefix> <fq_path_prefix>...
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

Args:
    <ref_path_prefix>       RSEM reference name
    <fq_path_prefix>        Path to a FASTQ file
"""

import logging
import os

from docopt import docopt
from ftarc.cli.util import fetch_executable, print_log
from psutil import cpu_count, virtual_memory

from .. import __version__


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
    genome_versions = {'GRCh38', 'GRCh37', 'GRCm39', 'GRCm38'}
    assert args['--genome'] in genome_versions, 'unsupprted genome version'
    print_log(f'Start the workflow of rnasa {__version__}')
    n_cpu = int(args['--cpus'] or cpu_count())
    memory_mb = virtual_memory().total / 1024 / 1024 / 2
    sh_config = {
        'log_dir_path': None,
        'remove_if_failed': (not args['--skip-cleaning']), 'quiet': False,
        'executable': fetch_executable('bash')
    }
    if args['download']:
        pass
    elif args['run']:
        pass
