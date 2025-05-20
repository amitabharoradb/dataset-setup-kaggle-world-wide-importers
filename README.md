# dataset-setup-kaggle-world-wide-importers
Project to create Kaggle's World Wide Importers dataset into you own Unity Catalog environment

[**Inspired by Robert Mosley's notenook**](https://e2-demo-field-eng.cloud.databricks.com/editor/notebooks/634720160573407?o=1444828305810485#command/634720160573408)

## Prerequisite: Kaggle Account Setup & dowload secrets file

#### Create Kaggle Account
Please go to *kaggle.com*, create an account, and download the credentials file.
See instructions [here](https://christianjmills.com/posts/kaggle-obtain-api-key-tutorial/)

The format of the Kaggle file will be

```json
{
  "username": "your kaggle username",
  "key": "your api key"
}
```

### Create Databricks secret scope if it does not exist

Use the Databricks CLI tool for below instructions.

Check if scope exists.  If yes then all good
```sh
databricks secrets list-scopes -p e2-demo-field-eng | grep amitabh_arora_scope
databricks secrets list-secrets amitabh_arora_scope -p e2-demo-field-eng
```

Otherwise create/update the scope.  **Note:** Due to limits you may have to delete an unused scope
```
databricks secrets create-scope amitabh_arora_scope -p e2-demo-field-eng
```

Create the keys now.  Copy the values from ~/Downloads/kaggle.json file
```
databricks secrets put-secret amitabh_arora_scope "kaggle_username" --string-value "amitabharora" -p e2-demo-field-eng
cat ~/Downloads/kaggle.json
databricks secrets put-secret amitabh_arora_scope "kaggle_key" --string-value "your api key" -p e2-demo-field-eng
```
