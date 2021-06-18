rnasa
=====

Gene Expression Level Calculator for RNA-seq

[![Test](https://github.com/dceoy/rnasa/actions/workflows/test.yml/badge.svg)](https://github.com/dceoy/rnasa/actions/workflows/test.yml)
[![Upload Python Package](https://github.com/dceoy/rnasa/actions/workflows/python-publish.yml/badge.svg)](https://github.com/dceoy/rnasa/actions/workflows/python-publish.yml)
[![CI to Docker Hub](https://github.com/dceoy/rnasa/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/dceoy/rnasa/actions/workflows/docker-publish.yml)

Installation
------------

```sh
$ pip install -U rnasa
```

Dependent commands:

- `pigz`
- `pbzip2`
- `bgzip`
- `samtools` (and `plot-bamstats`)
- `java`
- `fastqc`
- `trim_galore`
- `STAR`
- `rsem-prepare-reference`
- `rsem-refseq-extract-primary-assembly`
- `rsem-calculate-expression`

Docker image
------------

Pull the image from [Docker Hub](https://hub.docker.com/r/dceoy/rnasa/).

```sh
$ docker image pull dceoy/rnasa
```

Usage
-----

#### Calculate gene expression levels

| input files       | output files  |
|:-----------------:|:-------------:|
| FASTQ (Illumina)  | TSV (or GCT)  |


1.  Download and process resource data.

    ```sh
    $ rnasa download --genome=GRCh38 --dest-dir=/path/to/ref
    ```

2.  Calculate TPM (transcripts per million) values from FASTQ files.

    ```sh
    $ rnasa calculate \
        --workers=2 \
        --dest-dir=/path/to/output \
        /path/to/ref/GRCh38 \
        /path/to/sample1_fastq_prefix \
        /path/to/sample2_fastq_prefix \
        /path/to/sample3_fastq_prefix
    ```

    The command search for one (single-end) or two (paired-end) input FASTQ files by prefix.

    Standard workflow:
    1.  Trim adapters
        - `trim_galore`
    2.  Map reads and calculate TPM values
        - `STAR`
        - `rsem-calculate-expression`
    3.  Collect QC metrics
        - `fastqc`
        - `samtools`

3.  Extract TPM values from RSEM results files, and consolidate them into TSV files.

    ```sh
    $ rnasa extract --dest-dir=. /path/to/output/rsem
    ```

    If `--gct` is passed, `rnasa extract` creates output files in GCT format.

Run `rnasa --help` for more information.
