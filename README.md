# On Shapley Value in Data Assemblage Under Independent Utility

This repository contains the implementation of algorithms in our VLDB 2022 paper.

If you find the code here useful, please consider to cite our paper:

```bibtex
@article{PVLDB:shapley-value-under-independent-utility,
  author = {Luo, Xuan and Pei, Jian and Cong, Zicun and Xu, Cheng},
  title = {On Shapley Value in Data Assemblage Under Independent Utility},
  journal = {Proceedings of the VLDB Endowment},
  year = {2022},
  volume = {15},
  number = {11},
  pages = {2761--2773},
  issn = {2150-8097},
  doi = {10.14778/3551793.3551829}
}
```

## Install Dependencies
* OS: Ubuntu 20.04 LTS.
* Rust: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`

## Build

```bash
cargo build --release
```

## Source Data
Source data: https://drive.google.com/drive/folders/1cSQa4S_ughm7x8rOUljig59DcRQMVJL1?usp=sharing

## How to Run
1. Download source data
2. Go to scripts folder, generate metadata via:
```bash
python3 assign_data.py -d dataset -a <alpha> -b <beta> -k <number_of_data_owner> -m <max_copy> -o <equal owners> -r <equal records> -f <output dir>
```
Example:
```bash
python3 assign_data.py -d 'world' -a 4 -b 3 -k 10 -m 3 -o 1 -r 1 -f tmp/
```
3. Calculate Shapley value for all data owners:
```bash
shapley-value  -i <source data dir> -m <metadata dir/dataset> -d <dataset> -o <output file> -s <scheme>
```
Example:
```bash
./target/release/shapley-value  -i data/world -m metadata/world -d world -o proposed.json -s proposed
```

