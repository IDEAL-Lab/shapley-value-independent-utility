# On Shapley Value in Data Assemblage Under Independent Utility

This repo contains the implementation of algrithms in our paper. 

## Install Dependencies
OS: Ubuntu 20.04 LTS.
Rust: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh 

## Build
```bash
cargo check
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
    




