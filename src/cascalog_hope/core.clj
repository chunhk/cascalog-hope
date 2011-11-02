(ns cascalog-hope.core
  (:use cascalog.api)
  (:require [clojure-csv [core :as csv]])
  (:require [clj-json [core :as json]])
  (:require [cascalog [ops :as c]]))

(defmacro bootstrap []
  '(do
    (use (quote cascalog.api))
    (require (quote [clojure-csv [core :as csv]]))
    (require (quote [clj-json [core :as json]]))
    (require (quote [cascalog [ops :as c]]))))

;; if you wish to use files instead of sequences, you can uncomment these lines and comment out the
;; other cities, buildings, buildings-text definitions below
;; cascalog will default to the local filesystem if not running on a hadoop cluster

;(def cities (hfs-textline "resources/cities"))
;(def buildings (hfs-textline "resources/buildings"))
;(def buildings-text (hfs-textline "resources/buildings-text"))

(def cities
  ["New York, NY"
   "Chicago, IL"
   "Los Angeles, CA"
   "San Francisco, CA"
   "Seattle, WA"])

(def buildings
  ["{\"name\":\"Chrysler Building\",\"city\":\"New York\"}"
   "{\"name\":\"Empire State Building\",\"city\":\"New York\"}"
   "{\"name\":\"John Hancock Center\",\"city\":\"Chicago\"}"
   "{\"name\":\"Walt Disney Concert Hall\",\"city\":\"Los Angeles\"}"
   "{\"name\":\"Transamerica Pyramid\",\"city\":\"San Francisco\"}"
   "{\"name\":\"Space Needle\",\"city\":\"Seattle\"}"])

(def buildings-text
  ["The Chrysler Building is located in New York" 
   "The Empire State Building is located in New York" 
   "The John Hancock Center is located in Chicago" 
   "The Walt Disney Concert Hall is located in Los Angeles" 
   "The Transamerica Pyramid is located in San Francisco"
   "The Space Needle is located in Seattle"])

(defn cities-parser 
  "function that parses city,state string into city and state"
  [line]
  (map #(.trim %) (first (csv/parse-csv line))))

(defn city-state-query 
  "query that outputs all city and state pairs from cities dataset; repl usage: (city-state-query)"
  []
  (?<- (stdout) [?city ?state]
    (cities ?line)
    (cities-parser ?line :> ?city ?state)))

(defn california-query 
  "query that only outputs city and state pairs where state is 'CA'; repl usage: (california-query)"
  []
  (?<- (stdout) [?city ?state]
    (cities ?line)
    (cities-parser ?line :> ?city ?state)
    (= ?state "CA")))

(defn state-query 
  "parameterized form of state query; repl usage: (state-query 'CA')"
  [state]
  (?<- (stdout) [?city ?state]
    (cities ?line)
    (cities-parser ?line :> ?city ?state)
    (= ?state state)))

(defn buildings-parser
  "function that parse out the name and city from a json string"
  [line]
  (map (json/parse-string line) ["name" "city"]))

(defn building-city-query
  "query that outputs name and city from buildings dataset; repl usage: (building-city-query)"
  []
  (?<- (stdout) [?name ?city]
    (buildings ?line)
    (buildings-parser ?line :> ?name ?city)))

(defn join-buildings-cities
  "query that joins the buildings and cities datasets on city, to get name, city, state; repl usage: (join-buildings-city)"
  []
  (?<- (stdout) [?name ?city ?state] 
    (buildings ?building_line) 
    (buildings-parser ?building_line :> ?name ?city) 
    (cities ?city_line) 
    (cities-parser ?city_line :> ?city ?state)))

(defn buildings-text-parser
  "function to parse out the city from a raw text string" 
  [line]
  (map #(nth (first (re-seq #"The .* is located in (.*)" line)) %) [1]))

; subquery that will be used as datasource
(def cities-subquery
  (<- [?city ?state]
    (cities ?line)
    (cities-parser ?line :> ?city ?state)))

(defn buildings-per-city 
  "query to count the number of buildings per city; repl usage: (buildings-per-city)"
  []
  (?<- (stdout) [?state ?count]
    (buildings-text ?line) 
    (buildings-text-parser ?line :> ?city) 
    (cities-subquery ?city ?state) 
    (c/count ?count)))
