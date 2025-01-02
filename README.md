zap [![License](https://img.shields.io/badge/license-MIT-8FBD08.svg)](https://shields.io/)
====
Designing efficient task scheduling for Ziglang.

V1: https://github.com/kprotty/zap/tree/blog

V2: https://x.com/kingprotty/status/1873793282612060580

### Benchmark

```sh
$ zig build run -Doptimize=ReleaseFast
```

optionally set number of threads in the pools:

```sh
$ zig build run -Doptimize=ReleaseFast -- 8
```