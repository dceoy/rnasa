rnasa
=====

Gene Expression Level Calculator for RNA-seq

[![wercker status](https://app.wercker.com/status/a0ed10099e81e5f004b6a5a3d826312b/s/main "wercker status")](https://app.wercker.com/project/byKey/a0ed10099e81e5f004b6a5a3d826312b)
![Upload Python Package](https://github.com/dceoy/rnasa/workflows/Upload%20Python%20Package/badge.svg)

Installation
------------

```sh
$ pip install -U rnasa
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

2.  Calculate TPM (transcripts per million) values.

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

3.  Extract TPM values from RSEM results files.

    ```sh
    $ rnasa extract --dest-dir=. /path/to/output/rsem
    ```

Run `rnasa --help` for more information.
