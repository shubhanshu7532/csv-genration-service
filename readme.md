# Data format
{
    "table_name": "transactions",
    "query": {"status": "SUCCESS"},
    "column_mapping": {
      "amount": "Amount",
    }
  }

# How to run
1. pip install -r requirements.txt
2. python sqs_mongo_to_s3_service.py
