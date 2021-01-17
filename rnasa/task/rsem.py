#!/usr/bin/env python

import sys
from pathlib import Path
from socket import gethostname

import luigi

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


class DownloadAndPrepareRsemReferenceFiles(RnasaTask):
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
                f'{self.genome_version}.transcripts.fa',
                f'{self.genome_version}_STARtmp', 'Log.out', 'chrLength.txt',
                'chrName.txt', 'chrNameLength.txt', 'chrStart.txt',
                'exonGeTrInfo.tab', 'exonInfo.tab', 'geneInfo.tab',
                'sjdbList.fromGTF.out.tab', 'transcriptInfo.tab'
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
        ref_prefix = str(dest_dir.joinpath(f'rsem.star.{self.genome_version}'))
        bin_dir = Path(self.rsem_calculate_expression).resolve().parent
        rsem_refseq_extract_primary_assembly = bin_dir.joinpath(
            'rsem-refseq-extract-primary-assembly'
        )
        rsem_prepare_reference = bin_dir.joinpath('rsem-prepare-reference')
        self.setup_shell(
            run_id=run_id,
            commands=[
                self.rsem_calculate_expression, self.star, self.pigz,
                sys.executable
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
                + f'--star --num-threads {self.n_cpu}'
                + f' --gtf {gtf}'
                + f' {pa_fna} {ref_prefix}'
            ),
            input_files_or_dirs=pa_fna,
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


if __name__ == '__main__':
    luigi.run()
