# Data

This directory contains the dataset used to train the fraud-detection model and simulate a real-time credit-card transaction stream.

## Dataset source

The project uses the **Credit Card Fraud Detection** dataset published on Kaggle:

https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud

The dataset contains anonymized credit-card transactions labeled as legitimate or fraudulent. The original transaction attributes were transformed using Principal Component Analysis to protect confidential information.

## Dataset summary

The dataset contains:

- 284,807 transactions
- 492 fraudulent transactions
- 284,315 legitimate transactions
- 31 columns
- A fraud rate of approximately 0.172%
- Transactions collected over approximately two days

The dataset is highly imbalanced. As a result, accuracy is not used as the primary model-performance metric. The project focuses on precision, recall, F1-score, PR-AUC, ROC-AUC, and false positives per 1,000 transactions.

## File

After downloading and extracting the dataset, the following file should be available:

```text
creditcard.csv