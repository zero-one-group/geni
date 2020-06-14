(ns zero-one.geni.experimental.google-sheets
  (:require
    [clojure.java.io]
    [zero-one.geni.core :as g])
  (:import
    (com.google.api.client.googleapis.auth.oauth2 GoogleCredential)
    (com.google.api.client.googleapis.javanet GoogleNetHttpTransport)
    (com.google.api.client.json.jackson2 JacksonFactory)
    (com.google.api.services.sheets.v4 Sheets$Builder
                                       SheetsScopes)))

(def json-factory (JacksonFactory/getDefaultInstance))

(def http-transport (GoogleNetHttpTransport/newTrustedTransport))

(defn- google-credentials [creds-path]
  (-> (GoogleCredential/fromStream (clojure.java.io/input-stream creds-path))
      (.createScoped [SheetsScopes/SPREADSHEETS_READONLY])))

(defn sheets-service [google-props]
  (let [app-name       (:app-name google-props "Geni Lib")
        credentials    (google-credentials (:credentials google-props))]
    (-> (Sheets$Builder. http-transport json-factory credentials)
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
    (g/table->dataset spark rows col-names)))

(defn read-sheets! [spark google-props options]
  (let [service        (sheets-service google-props)
        values         (sheet-values service
                                     (:spreadsheet-id google-props)
                                     (:sheet-name google-props))]
    (spreadsheet-values->dataset spark values options)))
