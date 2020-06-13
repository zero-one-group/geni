(ns zero-one.geni.google-sheets-test
  (:require
    [zero-one.geni.core :as g]
    [midje.sweet :refer [facts =>]]
    [zero-one.geni.experimental.google-sheets :as gs]
    [zero-one.geni.test-resources :refer [spark]]))

(facts "On google sheets"
  (let [dataset (gs/read-sheets! spark
                                 {:credentials    "resources/credentials.json"
                                  :spreadsheet-id "1Kit0_YS1RMM1wBL0MksG-0SGuUpni-hbjqKhqL61XYc"})]
    (g/count dataset) => 30
    (g/column-names dataset) => ["Student Name"
                                 "Gender"
                                 "Class Level"
                                 "Home State"
                                 "Major"
                                 "Extracurricular Activity"]))
