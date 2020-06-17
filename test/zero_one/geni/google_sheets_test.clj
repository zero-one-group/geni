(ns zero-one.geni.google-sheets-test
  (:require
    [midje.sweet :refer [facts throws =>]]
    [zero-one.geni.core :as g]
    [zero-one.geni.experimental.google-sheets :as gs]
    [zero-one.geni.test-resources :refer [spark df-20]]))

(def google-props
  {:credentials "resources/credentials.json"})

(def dataframe
  (-> df-20 (g/select "SellerG" "Date" "Rooms" "Price")))

(defonce service (gs/sheets-service google-props))

(defn action-fn [retries-to-success]
  (let [n-tries (atom 0)]
    (fn []
      (swap! n-tries inc)
      (if (<= @n-tries retries-to-success)
        (throw (Exception. "Try again"))
        @n-tries))))

(facts "On exponential-backoff"
  (gs/exponential-backoff {:wait-ms     100
                           :growth-rate 2
                           :max-ms      399
                           :action!     (action-fn 0)}) => 1
  (gs/exponential-backoff {:wait-ms     100
                           :growth-rate 2
                           :max-ms      399
                           :action!     (action-fn 1)}) => 2
  (gs/exponential-backoff {:wait-ms     100
                           :growth-rate 2
                           :max-ms      399
                           :action!     (action-fn 2)}) => 3
  (gs/exponential-backoff {:wait-ms     100
                           :growth-rate 2
                           :max-ms      399
                           :action!     (action-fn 3)}) => (throws Exception))

(facts "On writing to Google Sheets" :slow
  (let [spreadsheet-id (gs/create-sheets! (assoc google-props :sheet-name "melbourne"))
        new-props      (merge google-props {:sheet-name "melbourne" :spreadsheet-id spreadsheet-id})
        read-df        (do
                         (gs/write-sheets! dataframe new-props {:header false})
                         (gs/read-sheets! spark new-props {:header false}))
        delete-status  (gs/delete-sheets! google-props spreadsheet-id)]
    spreadsheet-id => string?
    (g/count read-df) => 20
    (g/columns read-df) => [:_c0 :_c1 :_c2 :_c3]
    delete-status => nil?)
  (let [spreadsheet-id (gs/create-sheets! google-props)
        new-props      (assoc google-props :spreadsheet-id spreadsheet-id)
        read-df        (do
                         (gs/write-sheets! dataframe new-props)
                         (gs/read-sheets! spark new-props))
        delete-status  (gs/delete-sheets! google-props spreadsheet-id)]
    spreadsheet-id => string?
    (g/count read-df) => 20
    (g/columns read-df) => [:SellerG :Date :Rooms :Price]
    delete-status => nil?))
