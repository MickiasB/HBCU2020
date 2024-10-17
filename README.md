
# HCSB Project

Welcome to the HCSB Project!

## Prerequisites

Before you get started, ensure you have the following prerequisites installed:

- [Python 3](https://www.python.org/downloads/)
- [virtualenv](https://pypi.org/project/virtualenv/)
- [Git](https://git-scm.com/)
- [node/Npm](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm)
s
## Getting Started

Follow these step-by-step instructions to set up and run the project:

### 1. Create a Parent Directory

```bash
mkdir hcsb_project
cd hcsb_project
```

### 2. Create Virtualn Env
```bash
pip install virtualenv
python3 -m venv env
source env/bin/activate
```

### 3. Clone Repository

```bash
git clone <repoURL>
cd HCSB2020
pip3 install -r requirements.txt
```

### 4. Install Node/npm for Dynamodb GUI
This is for everyone who doesn't have node/npm package. Inorder to check if you have it or not, run `node -v` and if it shows the version number you have it and you're good. If you see sth else, you need to follow this step

- Install nvm package using `brew`
```bash
brew install nvm
```
- After that, you need to put these lines of code into your terminal profile 
(either `vim ~/.zshrc` or `vim ~/.bash_profile`):
```bash
export NVM_DIR="$HOME/.nvm"
  [ -s "/opt/homebrew/opt/nvm/nvm.sh" ] && \. "/opt/homebrew/opt/nvm/nvm.sh"  # This loads nvm
  [ -s "/opt/homebrew/opt/nvm/etc/bash_completion.d/nvm" ] && \. "/opt/homebrew/opt/nvm/etc/bash_completion.d/nvm"  # This loads nvm bash_completion
```
- After that restart your terminal and on an a new terminal, run these commands:
```bash
nvm install 10.3.0
npm install -g yarn
```

### 5. Build Dynamodb server
Open another terminal and run the following command to set up the DynamoDB locally:

```bash
make dynamodb-local
```
- On another terminal, setup dynamodb GUI:
```bash
npm install -g dynamodb-admin #this pulls dynamodb-admin for the GUI
make dynamodb-admin
```

### 6. Data ingestion
Run the command to collect data and store it in a JSON file:

```bash
make collect-data

```
To ingest data into DynamoDB, run:

```bash
make ingest-data
```

### 7. Run API queries
Run the command to start the graphql and play with the queries:
```bash
make graphql
```

### 8. Run the S3 bucket locally
Run the command to start the S3 bucket locally:
```bash
make s3-local
```
Here you have to put the name of the directory, username and password to setup the S3 bucket locally.