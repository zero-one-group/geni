(ns examples.sheets
  (:require
    [zero-one.geni.core :as g]
    [zero-one.geni.test-resources :refer [spark df-20]]))

;; Setting up credentials:
;; https://github.com/SparkFund/google-apps-clj#obtaining-oauth-2-credentials

(require '[clojure.java.io])
(import '(com.google.api.services.sheets.v4.model Spreadsheet
                                                  SpreadsheetProperties))
(import '(com.google.api.client.extensions.java6.auth.oauth2 AuthorizationCodeInstalledApp))
(import '(com.google.api.client.extensions.jetty.auth.oauth2 LocalServerReceiver$Builder))
(import '(com.google.api.client.googleapis.auth.oauth2 GoogleAuthorizationCodeFlow$Builder))
(import '(com.google.api.client.googleapis.auth.oauth2 GoogleClientSecrets))
(import '(com.google.api.client.googleapis.javanet GoogleNetHttpTransport))
(import '(com.google.api.client.json.jackson2 JacksonFactory))
(import '(com.google.api.client.util.store FileDataStoreFactory))
(import '(com.google.api.services.sheets.v4 SheetsScopes))

(def json-factory (JacksonFactory/getDefaultInstance))

(def client-secrets
  (GoogleClientSecrets/load
    json-factory
    (clojure.java.io/reader "resources/credentials.json")))
(def http-transport (GoogleNetHttpTransport/newTrustedTransport))

(def tokens-path "resources/tokens")
(def flow
  (-> (GoogleAuthorizationCodeFlow$Builder. http-transport
                                            json-factory
                                            client-secrets
                                            [SheetsScopes/SPREADSHEETS_READONLY])
      (.setDataStoreFactory (FileDataStoreFactory. (java.io.File. tokens-path)))
      (.setAccessType "offline")
      .build))

(def receiver
  (-> (LocalServerReceiver$Builder.)
      (.setPort 8888)
      .build))

;; Note: authenticate on browser
(def credentials
  (-> (AuthorizationCodeInstalledApp. flow receiver)
      (.authorize "user")))


(import '(com.google.api.services.sheets.v4 Sheets$Builder));

(def app-name "Geni Test App")
(def service
  (-> (Sheets$Builder. http-transport
                       json-factory
                       credentials)
      (.setApplicationName app-name)
      .build))


(def spreadsheet-id "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms")
(def spreadsheet-range "Class Data!A2:E")

;; NOTE: enable google sheet API
(def response
  (-> service
      .spreadsheets
      .values
      (.get spreadsheet-id spreadsheet-range)
      .execute))

(let [rows (.getValues response)
      col-names [:name :gender :year :state :major]]
  (g/show (g/table->dataset spark rows col-names)))

(require '[clojure.reflect :as r])
(->> (r/reflect GoogleAuthorizationCodeFlow$Builder)
     :members)
