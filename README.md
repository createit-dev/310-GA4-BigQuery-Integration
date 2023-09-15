## Google Analytics 4 Data Fetching and BigQuery Integration

This Python script fetches data from GA4 and then saved it with Google's BigQuery. The data is sorted, filtered, and stored in `output.csv` file and is also sent to a specific BigQuery table.

### Features:

1. Fetches active users and event data based on a specified date range.
2. Provides options to fetch data from yesterday or from a wider initial date range.
3. Checks and prevents data duplication in BigQuery.
4. Supports dynamic table creation in BigQuery based on data date.
5. Outputs fetched data to `output.csv`.

### Requirements:

- Google Analytics Data API V1 Beta
- Google OAuth 2.0
- Google BigQuery Client
- Google Cloud exceptions
- argparse
- datetime
- sys
- json
- os
- pickle

### Configuration:

Before running the script, ensure you have a `config.json` file in the same directory with the following structure:

```json
{
    "CLIENT_SECRET_FILE": "<Path to your client secret file>",
    "SERVICE_ACCOUNT_FILE": "<Path to your service account JSON file>",
    "SCOPES": ["https://www.googleapis.com/auth/analytics.readonly"],
    "TABLE_PREFIX": "<Prefix for the BigQuery tables>",
    "PROPERTY_ID": "<Google Analytics Property ID>",
    "DATASET_ID": "<BigQuery Dataset ID>",
    "INITIAL_FETCH_FROM_DATE": "2022-01-01"
}
```

### Usage:

Run the script with one of the following flags:

- `--yesterday`: To fetch data from yesterday only.
- `--initial_fetch`: To fetch data from the initial date specified in the configuration up to today.

For example:

```bash
python ga4script.py --yesterday
```

**Note**: Using the `--initial_fetch` might result in duplicated records in BigQuery, so the script will ask for confirmation before proceeding.

### Output:

The script will output the fetched data to `output.csv`. The data is also saved to BigQuery, with tables dynamically created based on the data's month and year.


### Important Considerations:

1. Ensure you have set up the correct permissions for your Google service account to read data from Google Analytics and to write to BigQuery.
2. Keep your `config.json`, client secret, and service account JSON files confidential.
3. Handle the `token.pickle` file (generated after authentication) with care, as it contains your authentication token.

---

### BigQuery Storage Details

#### Table & Database

The data fetched by the script is stored in Google BigQuery, a fully-managed and serverless data warehouse. BigQuery allows super-fast SQL queries using the processing power of Google's infrastructure.

The script references a specific dataset, represented by the `DATASET_ID` in the configuration. Within this dataset, tables are dynamically created based on the data's month and year.

#### Naming Convention

The tables created have a name based on a prefix and the month and year of the data. This naming convention is structured as:

```
<TABLE_PREFIX><YEAR><MONTH>01
```

- `<TABLE_PREFIX>`: A constant prefix set in the configuration (`config.json`). It aids in distinguishing tables related to this script from others in the same dataset.

- `<YEAR>` and `<MONTH>`: Derived from the date of the fetched data, ensuring data is grouped by month in separate tables.

For instance, if your `TABLE_PREFIX` is `GAData_` and the data is from September 2023, the table name would be `GAData_20230901`.

#### Data Storage

Each table has a specific schema, with the following fields:

- `Event_Name`: STRING
- `Event_Date`: INTEGER
- `Event_Count`: INTEGER
- `Is_Conversion`: BOOLEAN
- `Channel`: STRING

When the script processes the fetched data, it checks whether a table for the respective month already exists. If not, it creates a new table with the aforementioned schema. As the script parses through the data, it structures each record to match this schema before insertion.

The script employs an intelligent mechanism to avoid data duplication. Before attempting to insert a new record, it checks if an identical record already exists in the table. If such a record is found, it skips the insertion for that particular data point.

---

### Troubleshooting

#### 1. Error:
```
503 Getting metadata from plugin failed with error: ('invalid_grant: Token has been expired or revoked.', {'error': 'invalid_grant', 'error_description': 'Token has been expired or revoked.'})
```
**Solution**: Delete the `token.pickle` file from your directory and run the script again to refresh it.

#### 2. Error:
```
Access blocked: This app’s request is invalid, Error 400: redirect_uri_mismatch
```
**Solution**: Click on "error details" when you see this message. You'll be provided with a URL, typically in the format `http://localhost:8080/`. Copy this URL, go to your Google Cloud Console, navigate to `APIs and services > Credentials > OAuth 2.0 Client IDs`, and add this URL under both "Authorised redirect URIs" and "Authorised JavaScript origins".

#### 3. Error:
```
Error 400: redirect_uri_mismatch. You can't sign in to this app because it doesn't comply with Google's OAuth 2.0 policy.
```
**Solution**: When you see this error in your console, copy the URL provided and paste it directly into your browser's address bar.

---


### Script Output

When you run the script with the `--yesterday` flag, you might see an output similar to the following:

```
λ python ga4script.py --yesterday
Starting fetching data from 2023-09-14 to 2023-09-14.
Authentication successful!
..record already exists in BigQuery (1)
..record already exists in BigQuery (1)
..record already exists in BigQuery (1)
Data saved to output.csv!
```

This output indicates the script's operations. The "..record already exists in BigQuery" messages are informing you that the data for those records already exists in your BigQuery table, and therefore, it's not adding duplicate entries. At the end of the script, the fetched data is saved to `output.csv`.


When you run the script with the `--initial_fetch` flag, you might see an output similar to the following:

```
λ python ga4script.py --initial_fetch
Using the initial_fetch might result in duplicated records. Do you want to proceed? (yes/no): yes
Starting fetching data from 2022-01-01 to 2023-09-15.
Authentication successful!
Data saved to output.csv!
Table test_backfill_v4_20221201 created.
Data saved to BigQuery for 12/2022!
Table test_backfill_v4_20230101 created.
Data saved to BigQuery for 01/2023!
Table test_backfill_v4_20230201 created.
Data saved to BigQuery for 02/2023!
Table test_backfill_v4_20230301 created.
Data saved to BigQuery for 03/2023!
Table test_backfill_v4_20230401 created.
Data saved to BigQuery for 04/2023!
Table test_backfill_v4_20230501 created.
Data saved to BigQuery for 05/2023!
Table test_backfill_v4_20230601 created.
Data saved to BigQuery for 06/2023!
Table test_backfill_v4_20230701 created.
Data saved to BigQuery for 07/2023!
Table test_backfill_v4_20230801 created.
Data saved to BigQuery for 08/2023!
Data saved to BigQuery for 09/2023!
``` 

### BigQuery preview

After script execution - data will appear in BigQuery.

![gC7nW7I06i.jpg](imgs%2FgC7nW7I06i.jpg)

![nN0geT24M2.jpg](imgs%2FnN0geT24M2.jpg)
