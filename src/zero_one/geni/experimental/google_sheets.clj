(ns zero-one.geni.experimental.google-sheets
  (:require
    [clojure.java.io]
    [zero-one.geni.dataset :as ds])
  (:import
    (com.google.api.client.googleapis.auth.oauth2 GoogleCredential)
    (com.google.api.client.googleapis.javanet GoogleNetHttpTransport)
    (com.google.api.client.json.jackson2 JacksonFactory)
    (com.google.api.services.drive DriveScopes Drive$Builder)
    (com.google.api.services.sheets.v4 Sheets$Builder
                                       SheetsScopes)
    (com.google.api.services.sheets.v4.model Sheet
                                             SheetProperties
                                             Spreadsheet
                                             SpreadsheetProperties
                                             ValueRange)))

(def json-factory (JacksonFactory/getDefaultInstance))

(def http-transport (GoogleNetHttpTransport/newTrustedTransport))

(defn- google-credentials [creds-path]
  (-> (GoogleCredential/fromStream (clojure.java.io/input-stream creds-path))
      (.createScoped [SheetsScopes/SPREADSHEETS DriveScopes/DRIVE])))

(defn sheets-service [google-props]
  (let [app-name       (:app-name google-props "Geni Lib")
        credentials    (google-credentials (:credentials google-props))]
    (-> (Sheets$Builder. http-transport json-factory credentials)
        (.setApplicationName app-name)
        .build)))

(defn drive-service [google-props]
  (let [app-name       (:app-name google-props "Geni Lib")
        credentials    (google-credentials (:credentials google-props))]
    (-> (Drive$Builder. http-transport json-factory credentials)
        (.setApplicationName app-name)
        .build)))

(defn sheet-names [service spreadsheet-id]
  (let [sheet-objs (-> service
                       .spreadsheets
                       (.get spreadsheet-id)
                       .execute
                       .getSheets)]
    (map #(-> % .getProperties .getTitle) sheet-objs)))

(defn sheet-values [service spreadsheet-id sheet-name]
  (let [sheet-name (or sheet-name
                       (first (sheet-names service spreadsheet-id)))
        value-objs (-> service
                       .spreadsheets
                       .values
                       (.get spreadsheet-id sheet-name)
                       .execute
                       .getValues)]
    (map seq value-objs)))

(defn default-columns [n-cols]
  (->> (range)
       (map #(keyword (str "_c" %)))
       (take n-cols)))

(defn spreadsheet-values->dataset [spark values options]
  (let [header    (:header options true)
        col-names (if header
                    (first values)
                    (default-columns (count (first values))))
        rows      (if header
                    (rest values)
                    values)]
    (ds/table->dataset spark rows col-names)))

(defn read-sheets!
  ([spark google-props] (read-sheets! spark google-props {}))
  ([spark google-props options]
   (let [service        (sheets-service google-props)
         values         (sheet-values service
                                      (:spreadsheet-id google-props)
                                      (:sheet-name google-props))]
     (spreadsheet-values->dataset spark values options))))

(defn dataset->value-range [dataframe options]
  (let [col-names (ds/column-names dataframe)
        row-vals  (ds/collect-vals dataframe)
        values    (if (:header options true)
                    (conj row-vals col-names)
                    row-vals)]
    (-> (ValueRange.) (.setValues values))))

(defn write-sheets!
  ([dataframe google-props] (write-sheets! dataframe google-props {}))
  ([dataframe google-props options]
   (let [service     (sheets-service google-props)
         value-range (dataset->value-range dataframe options)
         sheet-range (str (:sheet-name google-props "Sheet1") "!A1")]
     (-> service
         .spreadsheets
         .values
         (.update (:spreadsheet-id google-props) sheet-range value-range)
         (.setValueInputOption "USER_ENTERED")
         .execute))))

(defn create-sheets! [google-props]
  (let [service            (sheets-service google-props)
        sheet-props        (-> (SheetProperties.)
                               (.setTitle (:sheet-name google-props "Sheet1")))
        target-sheet       (-> (Sheet.)
                               (.setProperties sheet-props))
        target-spreadsheet (-> (Spreadsheet.)
                               (.setProperties (SpreadsheetProperties.))
                               (.setSheets [target-sheet]))]
    (-> service
        .spreadsheets
        (.create target-spreadsheet)
        (.setFields "spreadsheetId")
        .execute
        .getSpreadsheetId)))

(defn delete-sheets! [google-props spreadsheet-id]
  (-> (drive-service google-props)
      .files
      (.delete spreadsheet-id)
      .execute))
