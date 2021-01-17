#!/usr/bin/env python

import sys
from pathlib import Path
from random import randint
from socket import gethostname

import luigi
from ftarc.task.trimgalore import PrepareFastqs

from .core import RnasaTask


class DownloadReferenceFiles(RnasaTask):
    src_urls = luigi.ListParameter()
    dest_dir_path = luigi.Parameter(default='.')
    run_id = luigi.Parameter(default=gethostname())
    wget = luigi.Parameter(default='wget')
    sh_config = luigi.DictParameter(default=dict())
    priority = 10

    def output(self):
        dest_dir = Path(self.dest_dir_path).resolve()
        return [
            luigi.LocalTarget(dest_dir.joinpath(Path(u).name))
            for u in self.src_urls
        ]

    def run(self):
        dest_dir = Path(self.dest_dir_path).resolve()
        self.print_log(f'Download reference files:\t{dest_dir}')
        self.setup_shell(
            run_id=self.run_id, commands=self.wget, cwd=dest_dir,
            **self.sh_config
        )
        for u in self.src_urls:
            o = dest_dir.joinpath(Path(u).name)
            self.run_shell(
                args=f'set -e && {self.wget} -qSL -O {o} {u}',
                output_files_or_dirs=o
            )


class PrepareRsemReferenceFiles(RnasaTask):
    fna_url = luigi.Parameter()
    gtf_url = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    genome_version = luigi.Parameter(default='GRCh38')
    wget = luigi.Parameter(default='wget')
    pigz = luigi.Parameter(default='pigz')
    star = luigi.Parameter(default='STAR')
    rsem_calculate_expression = luigi.Parameter(
        default='rsem-calculate-expression'
    )
    perl = luigi.Parameter(default='perl')
    n_cpu = luigi.IntParameter(default=1)
    sh_config = luigi.DictParameter(default=dict())
    priority = 10

    def requires(self):
        return DownloadReferenceFiles(
            src_urls=[self.fna_url, self.gtf_url],
            dest_dir_path=self.dest_dir_path, wget=self.wget,
            sh_config=self.sh_config
        )

    def output(self):
        dest_dir = Path(self.dest_dir_path).resolve()
        return [
            luigi.LocalTarget(dest_dir.joinpath(n)) for n in [
                f'{self.genome_version}.chrlist', f'{self.genome_version}.grp',
                f'{self.genome_version}.idx.fa',
                f'{self.genome_version}.n2g.idx.fa',
                f'{self.genome_version}.seq', f'{self.genome_version}.ti',
                f'{self.genome_version}.transcripts.fa', 'Log.out',
                'chrLength.txt', 'chrName.txt', 'chrNameLength.txt',
                'chrStart.txt', 'exonGeTrInfo.tab', 'exonInfo.tab',
                'geneInfo.tab', 'sjdbList.fromGTF.out.tab',
                'transcriptInfo.tab'
            ]
        ]

    def run(self):
        dest_dir = Path(self.dest_dir_path).resolve()
        fna_gz = Path(self.input()[0].path)
        fna = dest_dir.joinpath(
            fna_gz.stem if fna_gz.suffix == '.gz' else fna_gz.name
        )
        run_id = fna.stem
        self.print_log(f'Prepare RSEM references:\t{run_id}')
        gtf_gz = Path(self.input()[1].path)
        gtf = dest_dir.joinpath(
            gtf_gz.stem if gtf_gz.suffix == '.gz' else gtf_gz.name
        )
        ref_prefix = str(dest_dir.joinpath(self.genome_version))
        bin_dir = Path(self.rsem_calculate_expression).resolve().parent
        rsem_refseq_extract_primary_assembly = bin_dir.joinpath(
            'rsem-refseq-extract-primary-assembly'
        )
        rsem_prepare_reference = bin_dir.joinpath('rsem-prepare-reference')
        self.setup_shell(
            run_id=run_id,
            commands=[
                self.rsem_calculate_expression, self.star, self.pigz,
                sys.executable, self.perl
            ],
            cwd=dest_dir, **self.sh_config
        )
        for i, o in zip([fna_gz, gtf_gz], [fna, gtf]):
            if i.suffix == '.gz':
                self.run_shell(
                    args=f'set -e && {self.pigz} -p {self.n_cpu} -dk {i}',
                    input_files_or_dirs=i, output_files_or_dirs=o
                )
        if self.genome_version.startswith('GRCh'):
            pa_fna = dest_dir.joinpath(f'{fna.stem}.primary_assembly.fna')
            self.run_shell(
                args=(
                    f'set -e && {sys.executable}'
                    + f' {rsem_refseq_extract_primary_assembly} {fna} {pa_fna}'
                ),
                input_files_or_dirs=fna, output_files_or_dirs=pa_fna
            )
        else:
            pa_fna = fna
        self.run_shell(
            args=(
                f'set -e && {rsem_prepare_reference}'
                + ' --star'
                + f' --num-threads {self.n_cpu}'
                + f' --gtf {gtf}'
                + f' {pa_fna} {ref_prefix}'
            ),
            input_files_or_dirs=[pa_fna, gtf],
            output_files_or_dirs=[o.path for o in self.output()]
        )
        for o in {fna, gtf, pa_fna}:
            if Path(f'{o}.gz').is_file():
                self.remove_files_and_dirs(o)
            else:
                self.run_shell(
                    args=f'set -e && {self.pigz} -p {self.n_cpu} {o}',
                    input_files_or_dirs=o, output_files_or_dirs=f'{o}.gz'
                )


class CalculateTpmWithRsem(RnasaTask):
    fq_paths = luigi.ListParameter()
    ref_prefix = luigi.Parameter()
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
    priority = 20

    def requires(self):
        dest_dir = Path(self.dest_dir_path).resolve()
        return PrepareFastqs(
            fq_paths=self.fq_paths,
            sample_name=self.parse_fq_id(self.fq_paths[0]),
            cf={
                'adapter_removal': self.adapter_removal,
                'trim_dir_path': str(dest_dir.joinpath('trim')),
                'align_dir_path': str(dest_dir.joinpath('fq')),
                'pigz': self.pigz, 'pbzip2': self.pbzip2,
                'trim_galore': self.trim_galore, 'cutadapt': self.cutadapt,
                'fastqc': self.fastqc
            },
            n_cpu=self.n_cpu, memory_mb=self.memory_mb,
            sh_config=self.sh_config
        )

    def output(self):
        exp_dir = Path(self.dest_dir_path).resolve().joinpath(
            'expression'
        ).joinpath(self.parse_fq_id(self.fq_paths[0]))
        return luigi.LocalTarget(exp_dir)

    def run(self):
        dest_dir = Path(self.output().path)
        run_id = dest_dir.name
        self.print_log(f'Calculate TPM values:\t{run_id}')
        input_fqs = [Path(i.path) for i in self.input()]
        is_paired_end = (len(self.fq_paths) > 1)
        map_prefix = str(dest_dir.joinpath(f'{dest_dir.name}.rsem.star'))
        memory_mb_per_thread = int(self.memory_mb / self.n_cpu)
        self.setup_shell(
            run_id=run_id,
            commands=[self.rsem_calculate_expression, self.star], cwd=dest_dir,
            **self.sh_config
        )
        self.run_shell(
            args=(
                f'set -e && {self.rsem_calculate_expression}'
                + ' --star'
                + ' --estimate-rspd'
                + ' --append-names'
                + ' --sort-bam-by-coordinate'
                + ' --calc-pme'
                + ' --calc-ci'
                + ' --time'
                + ' --star-gzipped-read-file'
                + f' --num-threads {self.n_cpu}'
                + f' --sort-bam-memory-per-thread {memory_mb_per_thread}M'
                + f' --ci-memory {memory_mb_per_thread}'
                + f' --seed {self.seed}'
                + (' --paired-end' if is_paired_end else '')
                + ''.join(f' {f}' for f in input_fqs)
                + f' {self.ref_prefix} {map_prefix}'
            ),
            input_files_or_dirs=input_fqs,
            output_files_or_dirs=self.output().path
        )


if __name__ == '__main__':
    luigi.run()
