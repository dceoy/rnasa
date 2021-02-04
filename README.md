rnasa
=====

Gene Expression Analyzer for RNA-seq samples

[![wercker status](https://app.wercker.com/status/a0ed10099e81e5f004b6a5a3d826312b/s/main "wercker status")](https://app.wercker.com/project/byKey/a0ed10099e81e5f004b6a5a3d826312b)

Installation
------------

```sh
$ pip install -U https://github.com/dceoy/rnasa/archive/main.tar.gz
```

Dependent commands:

- `pigz`
- `pbzip2`
- `bgzip`
- `samtools`
- `java`
- `fastqc`
- `trim_galore`
- `STAR`
- `rsem-refseq-extract-primary-assembly`
- `rsem-prepare-reference`
- `rsem-calculate-expression`

Docker image
------------

Pull the image from [Docker Hub](https://hub.docker.com/r/dceoy/rnasa/).

```sh
$ docker image pull dceoy/rnasa
```

Usage
-----

1.  Download and process resource data.

    ```sh
    $ rnasa download --genome=GRCh38 --dest-dir=/path/to/ref
    ```

2.  Run the pipeline for gene expression analysis.

    ```sh
    $ rnasa run \
        --workers=2 \
        --dest-dir=. \
        /path/to/ref/GRCh38 \
        /path/to/sample1_fastq_prefix \
        /path/to/sample2_fastq_prefix \
        /path/to/sample3_fastq_prefix
    ```

    The command search for one (single-end) or two (paired-end) input FASTQ files by prefix.

Run `rnasa --help` for more information.
