## Optional Google Sheets Integration

Geni will automatically detect whether the following optional dependencies are present:

```clojure
[com.google.api-client/google-api-client "1.30.9"]
[com.google.apis/google-api-services-drive "v3-rev197-1.25.0"]
[com.google.apis/google-api-services-sheets "v4-rev612-1.25.0"]
[com.google.oauth-client/google-oauth-client-jetty "1.30.6"]
[org.apache.hadoop/hadoop-client "2.7.3"]
```

If so, the vars `g/read-sheets!` and `g/write-sheets!` will be bound to the appropriate functions. Otherwise, the vars will remain unbound.

Note that the additional Hadoop dependency is required to prevent a Guava compatibility conflict between Spark and Google APIs. For a potential fix without downgrading Hadoop client, see [here](https://github.com/googleapis/google-cloud-java/issues/4414).

In order to read and write from Google Sheets, we need to ensure that we have the following:

1. A [spreadsheet ID](https://developers.google.com/sheets/api/guides/concepts#spreadsheet_id). The sheet name is optional, and it defaults to `"Sheet1"`.
2. A project with [Google Sheets enabled](https://developers.google.com/sheets/api/quickstart/js) and a [service account](https://cloud.google.com/iam/docs/understanding-service-accounts) with a [private key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating_service_account_keys) stored in JSON.
3. Permissions to read and write for the service account. An easy way to ensure the right permissions is to share an existing spreadsheet to the [email of the service account](https://console.developers.google.com/apis/credentials).

With that in mind, the following snippets write and read from a particular sheet on a spreadsheet:

```clojure
(let [google-props {:credentials    "{path-to-api-key}"
                    :spreadsheet-id "{gsheet-spreadsheet-id}"
                    :sheet-name     "{sheet-name}"}
      options      {:header false}]
  (g/read-sheets! spark google-props options)
  (g/write-sheets! dataframe google-props options))
```
