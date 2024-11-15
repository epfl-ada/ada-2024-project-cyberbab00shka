# Repository structure

The project structure aligns with the proposed one:

```
├── data                        <- Project data files
│
├── src                         <- Source code
│   ├── data                            <- Data directory
│   ├── models                          <- Model directory
│   ├── utils                           <- Utility directory
│   ├── scripts                         <- Scripts
│   ├── notebooks                       <- Different supplementary notebooks
│
├── tests                       <- Tests of any kind
│
├── results.ipynb               <- a well-structured notebook showing the results
│
├── .gitignore                  <- List of files ignored by git
├── pip_requirements.txt        <- File for installing python dependencies
└── README.md
```

## Set up env

```
pip install -r pip_requirements.txt
```

## Get data

See results.ipynb to see how the data is downloaded. We do not commit large files here.
