
# matchminer-admin

A repository for preparing and managing clinical and trial data that is Matchminer compliant.
This system facilitates the ingestion, transformation, and uploading of data to the MatchMiner server, serving as a crucial administrative interface for clinical research data management.

## Getting Started

matchminer-admin provides command-line arguments for following tasks:

 - Trial data:
	 - Insert CTML compliant trial documents
	 - Get the highest/max protocol_id/protocol_number from the existing trials in matchminer system
	 - Get the list of NCT Ids for all existing trials in matchminer system that were pulled from clinicaltrials.gov 
 - Patient's clinical & genomic data:
   - Insert patient's clinical and genomic documents that comply with  matchminer schema

### Installation

 1.  Clone the repo
   ```sh
   git clone git@github.com:sumedhasaxena/matchminer-admin.git
   ```
   2. Make sure matchminer_server details are specified in `config.py`

## Usage
### 1. Insert trials in matchminer system

    python trial.py insert

### 2. Get max protocol_id/protocol_number from existing trials

    python trial.py get_max_pid_pno

### 3. Get a list of NCT ids for existing trials

    python trial.py get_all_nct_ids

### 4. Insert patient's clinical & genomic data in matchminer system

    python patient.py
