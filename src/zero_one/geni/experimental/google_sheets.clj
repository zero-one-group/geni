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

(defonce json-factory (JacksonFactory/getDefaultInstance))

(defonce http-transport (GoogleNetHttpTransport/newTrustedTransport))

(defn google-credentials [creds-path]
  (-> (GoogleCredential/fromStream (clojure.java.io/input-stream creds-path))
      (.createScoped [SheetsScopes/SPREADSHEETS_READONLY])))

(defn sheets-service [credentials app-name]
  (-> (Sheets$Builder. http-transport
                       json-factory
                       credentials)
      (.setApplicationName app-name)
      .build))

(defn first-sheet-name [service spreadsheet-id]
  (-> service
      .spreadsheets
      (.get spreadsheet-id)
      .execute
      .getSheets
      first
      .getProperties
      .getTitle))

(defn first-sheet-values [service spreadsheet-id]
  (-> service
      .spreadsheets
      .values
      (.get spreadsheet-id (first-sheet-name service spreadsheet-id))
      .execute
      .getValues))

(defn spreadsheet-values->dataset [spark values]
  (let [col-names (first values)
        rows      (rest values)]
    (g/table->dataset spark rows col-names)))

(defn read-sheets! [spark google-props]
  (let [app-name       (:app-name google-props "Geni Lib")
        credentials    (google-credentials (:credentials google-props))
        spreadsheet-id (:spreadsheet-id google-props)
        service        (sheets-service credentials app-name)]
    (spreadsheet-values->dataset spark (first-sheet-values service spreadsheet-id))))

(comment
  (require '[zero-one.geni.test-resources :refer [spark]])

  (-> (read-sheets! spark {:credentials    "resources/credentials.json"
                           :spreadsheet-id "1Kit0_YS1RMM1wBL0MksG-0SGuUpni-hbjqKhqL61XYc"})
      g/show)

  true)

