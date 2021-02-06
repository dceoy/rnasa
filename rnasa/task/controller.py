#!/usr/bin/env python

import sys
from itertools import product
from pathlib import Path
from random import randint
from socket import gethostname

import luigi
from ftarc.task.fastqc import CollectFqMetricsWithFastqc
from ftarc.task.samtools import CollectSamMetricsWithSamtools

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


class RunRnaseqPipeline(luigi.Task):
    fq_path_prefix = luigi.Parameter()
    ref_path_prefix = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    adapter_removal = luigi.BoolParameter(default=True)
    qc = luigi.BoolParameter(default=True)
    seed = luigi.IntParameter(default=randint(0, 2147483647))
    pigz = luigi.Parameter(default='pigz')
    pbzip2 = luigi.Parameter(default='pbzip2')
    trim_galore = luigi.Parameter(default='trim_galore')
    cutadapt = luigi.Parameter(default='cutadapt')
    fastqc = luigi.Parameter(default='fastqc')
    star = luigi.Parameter(default='STAR')
    rsem_calculate_expression = luigi.Parameter(
        default='rsem-calculate-expression'
    )
    samtools = luigi.Parameter(default='samtools')
    samtools_qc_commands = luigi.ListParameter(
        default=['coverage', 'flagstat', 'idxstats', 'stats']
    )
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
    priority = luigi.IntParameter(default=sys.maxsize)

    def requires(self):
        dest_dir = Path(self.dest_dir_path).resolve()
        return CalculateTpmWithRsem(
            fq_paths=self._find_fq_paths(),
            ref_path_prefix=self.ref_path_prefix,
            dest_dir_path=str(dest_dir.joinpath('rsem')),
            fq_dir_path=str(dest_dir.joinpath('fq')),
            adapter_removal=self.adapter_removal, seed=self.seed,
            pigz=self.pigz, pbzip2=self.pbzip2, trim_galore=self.trim_galore,
            cutadapt=self.cutadapt, fastqc=self.fastqc, star=self.star,
            rsem_calculate_expression=self.rsem_calculate_expression,
            n_cpu=self.n_cpu, memory_mb=self.memory_mb,
            sh_config=self.sh_config
        )

    def output(self):
        if self.qc:
            qc_dir = Path(self.dest_dir_path).resolve().joinpath('qc')
            return (
                [luigi.LocalTarget(i.path) for i in self.input()] + [
                    luigi.LocalTarget(qc_dir.joinpath(n))
                    for n in ['fastqc', 'samtools']
                ]
            )
        else:
            return self.input()

    def run(self):
        if self.qc:
            dest_dir = Path(self.dest_dir_path).resolve()
            yield [
                CollectFqMetricsWithFastqc(
                    input_fq_paths=[
                        i.path for i in self.input()
                        if i.path.endswith(('.fq.gz', '.fastq.gz'))
                    ],
                    dest_dir_path=f'{dest_dir}/qc/fastqc', fastqc=self.fastqc,
                    n_cpu=self.n_cpu, memory_mb=self.memory_mb,
                    sh_config=self.sh_config
                ),
                *[
                    CollectSamMetricsWithSamtools(
                        input_sam_path=[
                            o.path for o in self.output()
                            if o.path.endswith('.transcript.sorted.bam')
                        ][0],
                        fa_path='', dest_dir_path=f'{dest_dir}/qc/samtools',
                        samtools_commands=[c], samtools=self.samtools,
                        pigz=self.pigz, n_cpu=self.n_cpu,
                        sh_config=self.sh_config
                    ) for c in self.samtools_qc_commands
                ]
            ]

    def _find_fq_paths(self):
        hits = sorted(
            o for o in Path(self.fq_path_prefix).resolve().parent.iterdir()
            if o.name.startswith(Path(self.fq_path_prefix).name) and (
                o.name.endswith(('.fq', '.fastq')) or o.name.endswith(
                    tuple(
                        f'.{a}.{b}' for a, b
                        in product(['fq', 'fastq'], ['', 'gz', 'bz2'])
                    )
                )
            )
        )
        assert bool(hits), f'FASTQ files not found: {hits}'
        if len(hits) == 1:
            pass
        else:
            for a, b in zip(hits[0].stem, hits[1].stem):
                assert a == b or (a == '1' and b == '2'), 'invalid path prefix'
        return [str(o) for o in hits[:2]]


if __name__ == '__main__':
    luigi.run()
