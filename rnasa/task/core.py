#!/usr/bin/env python

from pathlib import Path

from ftarc.task.core import ShellTask


class RnasaTask(ShellTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def generate_version_commands(commands):
        for c in ([commands] if isinstance(commands, str) else commands):
            n = Path(c).name
            if n == 'wget':
                yield f'{c} --version | head -1'
            else:
                yield f'{c} --version'
