(ns zero-one.geni.google-sheets-test
  (:require
    [clojure.walk :refer [keywordize-keys]]
    [zero-one.geni.core :as g]
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.experimental.google-sheets :as gs]
    [zero-one.geni.test-resources :refer [spark]]))

(def google-props
  {:credentials    "resources/credentials.json"
   :spreadsheet-id "1Kit0_YS1RMM1wBL0MksG-0SGuUpni-hbjqKhqL61XYc"})

(defonce service (gs/sheets-service google-props))

(facts "On google sheet conversion functions" :slow
  (let [ss-id  (:spreadsheet-id google-props)
        values (gs/sheet-values service ss-id "seismic_bumps")]
    (g/columns (gs/spreadsheet-values->dataset spark values {}))
    => [:V1 :V2 :V3 :V4 :V5 :V6 :V7 :Class]
    (g/columns (gs/spreadsheet-values->dataset spark values {:header false}))
    => [:_c0 :_c1 :_c2 :_c3 :_c4 :_c5 :_c6 :_c7]))

(facts "On google sheet basic functions" :slow
  (let [ss-id (:spreadsheet-id google-props)]
    (fact "should retrieve correct sheet names"
      (gs/sheet-names service ss-id) => ["gsheet_api_tutorial" "seismic_bumps"])
    (fact "should retrieve correct values with specified sheet"
      (first (gs/sheet-values service ss-id "seismic_bumps"))
      => ["V1" "V2" "V3" "V4" "V5" "V6" "V7" "Class"])
    (fact "should retrieve correct values with unspecified sheet"
      (first (gs/sheet-values service ss-id nil))
      => ["Student Name"
          "Gender"
          "Class Level"
          "Home State"
          "Major"
          "Extracurricular Activity"])))

(fact "On read-sheets!" :slow
 (let [dataset (gs/read-sheets! spark google-props {})]
   (g/count dataset) => 30
   (-> dataset g/collect last) => (keywordize-keys
                                    {"Class Level"              "4. Senior"
                                     "Extracurricular Activity" "Debate"
                                     "Gender"                   "Male"
                                     "Home State"               "FL"
                                     "Major"                    "Math"
                                     "Student Name"             "Will"}))
 (let [dataset (gs/read-sheets! spark google-props {:header false})]
   (g/count dataset) => 31
   (-> dataset g/collect first) => {:_c0 "Student Name"
                                    :_c1 "Gender"
                                    :_c2 "Class Level"
                                    :_c3 "Home State"
                                    :_c4 "Major"
                                    :_c5 "Extracurricular Activity"}))
