rnasa
=====

RNA-seq Data Analyzer

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

Run `rnasa --help` for more information.
