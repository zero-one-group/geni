(ns examples.sheets
  (:require
    [zero-one.geni.experimental.google-sheets :as gs])
  (:import
    (com.google.api.services.sheets.v4 Sheets)
    (com.google.api.client.googleapis.auth.oauth2 GoogleCredential)
    (com.google.api.services.sheets.v4 Sheets$Builder
                                       SheetsScopes)))


(import '(com.google.api.services.sheets.v4.model Spreadsheet
                                                  SpreadsheetProperties));

(def google-props
  {:credentials    "resources/credentials.json"
   :spreadsheet-id "1Kit0_YS1RMM1wBL0MksG-0SGuUpni-hbjqKhqL61XYc"})

(def service (gs/sheets-service google-props))

(def placeholder
  (-> (Spreadsheet.)
      (.setProperties (SpreadsheetProperties.))))

(def spreadsheet
  (-> service
      .spreadsheets
      (.create placeholder)
      (.setFields "spreadsheetId")
      .execute))
spreadsheet

{"spreadsheetId" "1MO5TjuJPauxhKIM1AsC0JQgRbZ4GEp52qat9iGsYQm8"}

(import '(com.google.api.services.drive Drive$Builder))
(import '(com.google.api.services.drive.model Permission))

(defn google-credentials [creds-path]
  (-> (GoogleCredential/fromStream (clojure.java.io/input-stream creds-path))
      (.createScoped ["https://www.googleapis.com/auth/drive"])))

(def drive-service
  (-> (Drive$Builder.
        gs/http-transport
        gs/json-factory
        (google-credentials (:credentials google-props)))
      (.setApplicationName "Geni Lib")
      .build))

(def new-permission
  (-> (Permission.)
      (.setType "user")
      (.setEmailAddress "anthony@zero-one-group.com")
      (.setRole "writer")))

(-> drive-service
    .permissions
    (.create
      "1MO5TjuJPauxhKIM1AsC0JQgRbZ4GEp52qat9iGsYQm8"
      new-permission)
    .execute)
