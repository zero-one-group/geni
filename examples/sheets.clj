(ns examples.sheets
  (:require
    [zero-one.geni.core :as g]
    [zero-one.geni.experimental.google-sheets :as gs]
    [zero-one.geni.test-resources :refer [spark df-20]])
  (:import
    (com.google.api.services.sheets.v4 Sheets)
    (com.google.api.client.googleapis.auth.oauth2 GoogleCredential)
    (com.google.api.services.sheets.v4 Sheets$Builder
                                       SheetsScopes)))


(import '(com.google.api.services.sheets.v4.model Spreadsheet
                                                  Sheet
                                                  SheetProperties
                                                  SpreadsheetProperties));
(import '(com.google.api.services.sheets.v4.model UpdateValuesResponse));
(import '(com.google.api.services.sheets.v4.model ValueRange));

(def dataframe (-> df-20 (g/select "SellerG" "Date" "Rooms" "Price")))
(def column-names (g/column-names dataframe))
(def values (g/collect-vals dataframe))
(def sheet-name "Melbourne")


(def google-props
  {:credentials    "resources/credentials.json"
   :spreadsheet-id "1Kit0_YS1RMM1wBL0MksG-0SGuUpni-hbjqKhqL61XYc"})

(def service (gs/sheets-service google-props))

(def target-sheet
  (-> (Sheet.)
      (.setProperties (-> (SheetProperties.) (.setTitle sheet-name)))))
(.getProperties target-sheet)

(def target-spreadsheet
  (-> (Spreadsheet.)
      (.setProperties (SpreadsheetProperties.))
      (.setSheets [target-sheet])))

(def spreadsheet
  (-> service
      .spreadsheets
      (.create target-spreadsheet)
      (.setFields "spreadsheetId")
      .execute))
spreadsheet

(def spreadsheet-id "1OtgROqQdXt1XDlLWxXcL_maEEqwBYOntmaxwgrn5gLg")
(println "Open spreadsheet:")
(println (str "https://docs.google.com/spreadsheets/d/"
              spreadsheet-id))

(def body (-> (ValueRange.) (.setValues (conj values column-names))))
(def sheet-range (str (or sheet-name "Sheet1") "!A1"))

(type service)

(-> service
    .spreadsheets
    .values
    (.update spreadsheet-id sheet-range body)
    (.setValueInputOption "USER_ENTERED")
    .execute)

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
      spreadsheet-id
      new-permission)
    .execute)
