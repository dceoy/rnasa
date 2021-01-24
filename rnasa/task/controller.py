#!/usr/bin/env python

import sys
from itertools import product
from pathlib import Path
from random import randint
from socket import gethostname

import luigi

from .core import RnasaTask
from .rsem import CalculateTpmWithRsem


class PrintEnvVersions(RnasaTask):
    command_paths = luigi.ListParameter(default=list())
    run_id = luigi.Parameter(default=gethostname())
    sh_config = luigi.DictParameter(default=dict())
    __is_completed = False

    def complete(self):
        return self.__is_completed

    def run(self):
        self.print_log(f'Print environment versions:\t{self.run_id}')
        self.setup_shell(
            run_id=self.run_id, commands=self.command_paths, **self.sh_config
        )
        self.print_env_versions()
        self.__is_completed = True


class RunRnaseqPipeline(luigi.WrapperTask):
    fq_path_prefix = luigi.Parameter()
    ref_path_prefix = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    adapter_removal = luigi.BoolParameter(default=True)
    seed = luigi.IntParameter(default=randint(0, 2147483647))
    pigz = luigi.Parameter(default='pigz')
    pbzip2 = luigi.Parameter(default='pbzip2')
    trim_galore = luigi.Parameter(default='trim_galore')
    cutadapt = luigi.Parameter(default='cutadapt')
    fastqc = luigi.Parameter(default='fastqc')
    rsem_calculate_expression = luigi.Parameter(
        default='rsem-calculate-expression'
    )
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
    sample_name = luigi.Parameter()
    cf = luigi.DictParameter()
    samtools_qc_commands = luigi.ListParameter(
        default=['coverage', 'flagstat', 'idxstats', 'stats']
    )
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
    priority = luigi.IntParameter(default=sys.maxsize)

    def requires(self):
        return CalculateTpmWithRsem(
            fq_paths=self._find_fq_paths(), ref_prefix=self.ref_prefix,
            dest_dir_path=self.dest_dir_path,
            adapter_removal=self.adapter_removal, seed=self.seed,
            pigz=self.pigz, pbzip2=self.pbzip2, trim_galore=self.trim_galore,
            cutadapt=self.cutadapt, fastqc=self.fastqc,
            rsem_calculate_expression=self.rsem_calculate_expression,
            n_cpu=self.n_cpu, memory_mb=self.memory_mb,
            sh_config=self.sh_config
        )

    def output(self):
        return self.input()

    def _find_fq_paths(self):
        hits = sorted(
            o for o in Path(self.fq_path_prefix).resolve().parent.iterdir()
            if o.name.startswith(self.fq_path_prefix) and (
                o.name.endswith(('.fq', '.fastq')) or o.name.endswith(
                    tuple(
                        f'.{a}.{b}' for a, b
                        in product(['fq', 'fastq'], ['', 'gz', 'bz2'])
                    )
                )
            )
        )
        assert bool(hits), 'FASTQ files not found'
        if len(hits) == 1:
            pass
        else:
            for a, b in zip(hits[0].stem, hits[1].stem):
                assert a == b or (a == '1' and b == '2'), 'invalid path prefix'
        return [str(o) for o in hits[:2]]


if __name__ == '__main__':
    luigi.run()
