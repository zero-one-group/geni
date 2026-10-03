(ns zero-one.geni.spark-functions-test
  "Each function in `zero-one.geni.core.functions`' table, run against the
  same call in SQL, and Geni's coverage of Spark's `functions`."
  (:require
   [clojure.string :as string]
   [clojure.test :refer [deftest is testing]]
   [clojure.walk :as walk]
   [zero-one.geni.core :as g]
   [zero-one.geni.core.function-table :as function-table]
   [zero-one.geni.spark :as spark]
   [zero-one.geni.test-resources :refer [spark without-task-error-logs]])
  (:import
   (clojure.lang ExceptionInfo)
   (java.lang.reflect Method Modifier)
   (org.apache.spark.sql Column functions)))

(defn- at-least? [since]
  (let [needed  (mapv parse-long (string/split since #"\."))
        version (mapv parse-long (re-seq #"\d+" (spark/classpath-version)))]
    (not (neg? (compare (vec (take (count needed) version)) needed)))))

(defn- fixture
  "Three rows, with a column of each kind that the examples use."
  []
  (g/sql @spark
         (str "SELECT CAST(id AS INT) AS i, id * 10 AS l, id + 0.5D AS d, -1.5D AS neg, "
              "CAST(id - 2 AS DOUBLE) AS z, IF(id = 2, NULL, CAST(id AS INT)) AS n, "
              "'Hello World' AS s, '  hi  ' AS t, 'a,b,c' AS csv, '1234.5' AS num, "
              "id % 2 = 0 AS b, DATE'2026-10-03' + CAST(id AS INT) AS dt, "
              "timestamp_seconds(1791030896 + id * 3600) AS ts, '2026-10-03 12:34:56' AS tss, "
              "array(3, 1, 2) AS xs, array(2, NULL, 4) AS ys, map('a', 1, 'b', 2) AS m, "
              "array(named_struct('a', 1, 'b', 'x'), named_struct('a', 2, 'b', 'y')) AS structs, "
              "'{\"a\": 1, \"b\": [1, 2]}' AS j, '[1, 2, 3]' AS jarr, "
              "'https://user@spark.apache.org:8080/path?query=1&k=v#ref' AS url, "
              "'<a><b>1</b><b>2</b></a>' AS xml, encode('Spark', 'utf-8') AS bin, "
              "'abcdefghijklmnop' AS aeskey, id * 1000 AS ms, 'a%20b' AS encoded "
              "FROM range(1, 4)")))

(defn- comparable
  "Results as values that compare by content: byte arrays as vectors."
  [x]
  (walk/postwalk #(if (bytes? %) (vec %) %) x))

(defn- check-row
  "Runs `run` with the Geni function's column, through `wrap` when it's
  given, and the SQL's, and checks that they give the same results, or,
  below the function's Spark version, that the function throws an error that
  names it."
  [[fn-sym args sql wrap] run]
  (let [f     @(ns-resolve 'zero-one.geni.core fn-sym)
        since (:since (get @function-table/table fn-sym))]
    (if (and since (not (at-least? since)))
      (is (thrown-with-msg? ExceptionInfo #"needs Spark" (apply f args)) (str fn-sym))
      (try
        (let [[geni sql-result] (run ((or wrap identity) (apply f args)) (g/expr sql))]
          (is (= sql-result geni) (str fn-sym " " (pr-str args) " vs " sql)))
        (catch Exception e
          (is (nil? e) (str fn-sym " threw " (first (string/split-lines (str (ex-message e)))))))))))

(defn- ->string [column] (g/cast column "string"))

(defn- select-run [df]
  (fn [geni sql]
    (let [rows (g/collect (g/select df {:geni geni :sql sql}))]
      [(comparable (map :geni rows)) (comparable (map :sql rows))])))

(defn- agg-run [df]
  (fn [geni sql]
    (let [[row] (g/collect (g/agg df {:geni geni :sql sql}))]
      (comparable [(:geni row) (:sql row)]))))

(def ^:private scalar-examples
  "[function args sql], selected from the fixture."
  [['acosh [:d] "acosh(d)"]
   ['aes-decrypt [(g/aes-encrypt :s :aeskey) :aeskey] "aes_decrypt(aes_encrypt(s, aeskey), aeskey)"]
   ['aes-encrypt [:s :aeskey (g/lit "ECB")] "aes_encrypt(s, aeskey, 'ECB')"]
   ['array-append [:xs 9] "array_append(xs, 9)"]
   ['array-compact [:ys] "array_compact(ys)"]
   ['array-insert [:xs 2 9] "array_insert(xs, 2, 9)"]
   ['array-prepend [:xs :i] "array_prepend(xs, i)"]
   ['array-size [:xs] "array_size(xs)"]
   ['asinh [:d] "asinh(d)"]
   ['assert-true [(g/> :i 0)] "assert_true(i > 0)"]
   ['atanh [(g/lit 0.5)] "atanh(0.5)"]
   ['bit-count [:i] "bit_count(i)"]
   ['bit-get [:i 0] "bit_get(i, 0)"]
   ['bit-length [:s] "bit_length(s)"]
   ['bitmap-bit-position [:i] "bitmap_bit_position(i)"]
   ['bitmap-bucket-number [:i] "bitmap_bucket_number(i)"]
   ['bround [:d 0] "bround(d, 0)"]
   ['btrim [:t] "btrim(t)"]
   ['call-function ["upper" :s] "upper(s)"]
   ['call-udf ["lower" :s] "lower(s)"]
   ['cardinality [:xs] "cardinality(xs)"]
   ['ceil [:d 0] "ceil(d, 0)"]
   ['ceiling [:d] "ceiling(d)"]
   ['char [(g/+ :i 64)] "char(i + 64)"]
   ['char-length [:s] "char_length(s)"]
   ['character-length [:s] "character_length(s)"]
   ['chr [(g/+ :i 64)] "chr(i + 64)"]
   ['collate [:s "UNICODE_CI"] "collate(s, 'UNICODE_CI')"]
   ['collation [(delay (g/collate :s "UNICODE_CI"))] "collation(collate(s, 'UNICODE_CI'))"]
   ['convert-timezone [(g/lit "UTC") (g/lit "Asia/Jakarta") :ts] "convert_timezone('UTC', 'Asia/Jakarta', ts)"]
   ['cot [:d] "cot(d)"]
   ['csc [:d] "csc(d)"]
   ['curdate [] "curdate()"]
   ['current-catalog [] "current_catalog()"]
   ['current-database [] "current_database()"]
   ['current-path [] "current_path()"]
   ['current-schema [] "current_schema()"]
   ['current-timezone [] "current_timezone()"]
   ['current-user [] "current_user()"]
   ['date-from-unix-date [:i] "date_from_unix_date(i)"]
   ['date-part [(g/lit "YEAR") :dt] "date_part('YEAR', dt)"]
   ['dateadd [:dt 3] "dateadd(dt, 3)"]
   ['datepart [(g/lit "MONTH") :dt] "datepart('MONTH', dt)"]
   ['day [:dt] "day(dt)"]
   ['dayname [:dt] "dayname(dt)"]
   ['e [] "e()"]
   ['elt [2 :s :t] "elt(2, s, t)"]
   ['endswith [:s (g/lit "World")] "endswith(s, 'World')"]
   ['equal-null [:n :i] "equal_null(n, i)"]
   ['extract [(g/lit "DAY") :dt] "extract(DAY FROM dt)"]
   ['find-in-set [(g/lit "b") :csv] "find_in_set('b', csv)"]
   ['floor [:d 0] "floor(d, 0)"]
   ['from-utc-timestamp [:ts "Asia/Jakarta"] "from_utc_timestamp(ts, 'Asia/Jakarta')"]
   ['from-xml [:xml {:b [:int]}] "from_xml(xml, 'b ARRAY<INT>')"]
   ['get [:xs 1] "get(xs, 1)"]
   ['get-json-object [:j "$.b[1]"] "get_json_object(j, '$.b[1]')"]
   ['getbit [:i 1] "getbit(i, 1)"]
   ['ifnull [:n :i] "ifnull(n, i)"]
   ['input-file-block-length [] "input_file_block_length()"]
   ['input-file-block-start [] "input_file_block_start()"]
   ['is-valid-utf8 [:s] "is_valid_utf8(s)"]
   ['is-valid-variant [(delay (g/parse-json :j))] "is_valid_variant(parse_json(j))"]
   ['is-variant-null [(delay (g/parse-json (g/lit "null")))] "is_variant_null(parse_json('null'))"]
   ['isnan [:d] "isnan(d)"]
   ['isnotnull [:n] "isnotnull(n)"]
   ['isnull [:n] "isnull(n)"]
   ['java-method [(g/lit "java.lang.Math") (g/lit "abs") :i] "java_method('java.lang.Math', 'abs', i)"]
   ['json-array-length [:jarr] "json_array_length(jarr)"]
   ['json-object-keys [:j] "json_object_keys(j)"]
   ['lcase [:s] "lcase(s)"]
   ['left [:s 3] "left(s, 3)"]
   ['len [:s] "len(s)"]
   ['levenshtein [:s (g/lit "Hello") 3] "levenshtein(s, 'Hello', 3)"]
   ['ln [:d] "ln(d)"]
   ['localtimestamp [] "localtimestamp()"]
   ['locate ["o" :s 6] "locate('o', s, 6)"]
   ['log [2 :d] "log(2, d)"]
   ['make-date [2026 :i 3] "make_date(2026, i, 3)"]
   ['make-dt-interval [:i 2] "CAST(make_dt_interval(i, 2) AS STRING)" ->string]
   ['make-interval [1 :i] "CAST(make_interval(1, i) AS STRING)" ->string]
   ['make-time [:i 30 15.5] "CAST(make_time(i, 30, 15.5) AS STRING)" ->string]
   ['make-timestamp [2026 10 :i 12 30 15.5] "make_timestamp(2026, 10, i, 12, 30, 15.5)"]
   ['make-timestamp-ltz [2026 10 :i 12 30 15.5 (g/lit "UTC")] "make_timestamp_ltz(2026, 10, i, 12, 30, 15.5, 'UTC')"]
   ['make-timestamp-ntz [2026 10 :i 12 30 15.5] "make_timestamp_ntz(2026, 10, i, 12, 30, 15.5)"]
   ['make-valid-utf8 [:s] "make_valid_utf8(s)"]
   ['make-ym-interval [:i 2] "CAST(make_ym_interval(i, 2) AS STRING)" ->string]
   ['map-contains-key [:m "a"] "map_contains_key(m, 'a')"]
   ['mask [:s] "mask(s)"]
   ['monthname [:dt] "monthname(dt)"]
   ['months-between [:ts :dt false] "months_between(ts, dt, false)"]
   ['named-struct [(g/lit "x") :i (g/lit "y") :s] "named_struct('x', i, 'y', s)"]
   ['negative [:d] "negative(d)"]
   ['now [] "now()"]
   ['nullif [:i 2] "nullif(i, 2)"]
   ['nullifzero [:z] "nullifzero(z)"]
   ['nvl [:n 0] "nvl(n, 0)"]
   ['nvl2 [:n :i 0] "nvl2(n, i, 0)"]
   ['octet-length [:s] "octet_length(s)"]
   ['parse-url [:url (g/lit "QUERY") (g/lit "k")] "parse_url(url, 'QUERY', 'k')"]
   ['position [(g/lit "o") :s 6] "position('o', s, 6)"]
   ['positive [:neg] "positive(neg)"]
   ['power [:d 2] "power(d, 2)"]
   ['printf [(g/lit "%s and %d") :s :i] "printf('%s and %d', s, i)"]
   ['quote [:s] "quote(s)"]
   ['random [42] "random(42)"]
   ['randstr [5 42] "randstr(5, 42)"]
   ['reflect [(g/lit "java.lang.Math") (g/lit "abs") :i] "reflect('java.lang.Math', 'abs', i)"]
   ['regexp [:s (g/lit "o\\sW")] "regexp(s, 'o\\\\sW')"]
   ['regexp-count [:s (g/lit "o")] "regexp_count(s, 'o')"]
   ['regexp-extract-all [:s (g/lit "(\\w)o") 1] "regexp_extract_all(s, '(\\\\w)o', 1)"]
   ['regexp-instr [:s (g/lit "o")] "regexp_instr(s, 'o')"]
   ['regexp-like [:s (g/lit "^H")] "regexp_like(s, '^H')"]
   ['regexp-substr [:s (g/lit "W.r")] "regexp_substr(s, 'W.r')"]
   ['repeat [:s 2] "repeat(s, 2)"]
   ['replace-substring [:s (g/lit "o") (g/lit "0")] "replace(s, 'o', '0')"]
   ['right [:s 3] "right(s, 3)"]
   ['round [:d 0] "round(d, 0)"]
   ['sec [:d] "sec(d)"]
   ['sentences [:s] "sentences(s)"]
   ['sequence [1 :i] "sequence(1, i)"]
   ['session-user [] "session_user()"]
   ['sha [:s] "sha(s)"]
   ['shiftleft [:i 2] "shiftleft(i, 2)"]
   ['shiftright [:l 1] "shiftright(l, 1)"]
   ['shiftrightunsigned [:l 1] "shiftrightunsigned(l, 1)"]
   ['sign [:neg] "sign(neg)"]
   ['split [:csv "," 2] "split(csv, ',', 2)"]
   ['split-part [:csv (g/lit ",") 2] "split_part(csv, ',', 2)"]
   ['st-asbinary [(delay (g/st-geomfromwkb (g/unhex (g/lit "0101000000000000000000F03F0000000000000040"))))]
    "st_asbinary(st_geomfromwkb(unhex('0101000000000000000000F03F0000000000000040')))"]
   ['st-geogfromwkb [(g/unhex (g/lit "0101000000000000000000F03F0000000000000040"))]
    "st_asbinary(st_geogfromwkb(unhex('0101000000000000000000F03F0000000000000040')))" #(g/st-asbinary %)]
   ['st-geomfromwkb [(g/unhex (g/lit "0101000000000000000000F03F0000000000000040")) 4326]
    "st_srid(st_geomfromwkb(unhex('0101000000000000000000F03F0000000000000040'), 4326))" #(g/st-srid %)]
   ['st-setsrid [(delay (g/st-geomfromwkb (g/unhex (g/lit "0101000000000000000000F03F0000000000000040")))) 3857]
    "st_srid(st_setsrid(st_geomfromwkb(unhex('0101000000000000000000F03F0000000000000040')), 3857))" #(g/st-srid %)]
   ['st-srid [(delay (g/st-geomfromwkb (g/unhex (g/lit "0101000000000000000000F03F0000000000000040"))))]
    "st_srid(st_geomfromwkb(unhex('0101000000000000000000F03F0000000000000040')))"]
   ['startswith [:s (g/lit "Hell")] "startswith(s, 'Hell')"]
   ['str-to-map [(g/lit "a:1,b:2")] "str_to_map('a:1,b:2')"]
   ['substr [:s 2 3] "substr(s, 2, 3)"]
   ['time-diff [(g/lit "MINUTE") (delay (g/make-time 1 0 0)) (delay (g/make-time :i 30 0))]
    "time_diff('MINUTE', make_time(1, 0, 0), make_time(i, 30, 0))"]
   ['time-from-micros [(g/* :ms 1000000)] "CAST(time_from_micros(ms * 1000000) AS STRING)" ->string]
   ['time-from-millis [(g/* :ms 1000)] "CAST(time_from_millis(ms * 1000) AS STRING)" ->string]
   ['time-from-seconds [:ms] "CAST(time_from_seconds(ms) AS STRING)" ->string]
   ['time-to-micros [(delay (g/make-time :i 30 0))] "time_to_micros(make_time(i, 30, 0))"]
   ['time-to-millis [(delay (g/make-time :i 30 0))] "time_to_millis(make_time(i, 30, 0))"]
   ['time-to-seconds [(delay (g/make-time :i 30 0))] "time_to_seconds(make_time(i, 30, 0))"]
   ['time-trunc [(g/lit "HOUR") (delay (g/make-time :i 30 0))] "CAST(time_trunc('HOUR', make_time(i, 30, 0)) AS STRING)" ->string]
   ['timestamp-add ["HOUR" :i :ts] "timestampadd(HOUR, i, ts)"]
   ['timestamp-diff ["MINUTE" :ts (g/expr "ts + INTERVAL 90 MINUTES")]
    "timestampdiff(MINUTE, ts, ts + INTERVAL 90 MINUTES)"]
   ['timestamp-micros [:l] "timestamp_micros(l)"]
   ['timestamp-millis [:ms] "timestamp_millis(ms)"]
   ['timestamp-seconds [:ms] "timestamp_seconds(ms)"]
   ['to-binary [:s (g/lit "utf-8")] "to_binary(s, 'utf-8')"]
   ['to-char [:d (g/lit "999.9")] "to_char(d, '999.9')"]
   ['to-number [:num (g/lit "9999.9")] "to_number(num, '9999.9')"]
   ['to-time [(g/lit "12:34:56")] "CAST(to_time('12:34:56') AS STRING)" ->string]
   ['to-timestamp-ltz [:tss] "to_timestamp_ltz(tss)"]
   ['to-timestamp-ntz [:tss (g/lit "yyyy-MM-dd HH:mm:ss")] "to_timestamp_ntz(tss, 'yyyy-MM-dd HH:mm:ss')"]
   ['to-unix-timestamp [:tss] "to_unix_timestamp(tss)"]
   ['to-utc-timestamp [:ts "Asia/Jakarta"] "to_utc_timestamp(ts, 'Asia/Jakarta')"]
   ['to-varchar [:d (g/lit "999.9")] "to_varchar(d, '999.9')"]
   ['to-variant-object [(g/struct :i :s)] "to_json(to_variant_object(struct(i, s)))" #(g/to-json %)]
   ['to-xml [(g/struct :i :s)] "to_xml(struct(i, s))"]
   ['trunc [:dt "MM"] "trunc(dt, 'MM')"]
   ['try-add [:i :l] "try_add(i, l)"]
   ['try-aes-decrypt [:bin :aeskey] "try_aes_decrypt(bin, aeskey)"]
   ['try-divide [:i :z] "try_divide(i, z)"]
   ['try-element-at [:xs 5] "try_element_at(xs, 5)"]
   ['try-make-interval [1 :i] "CAST(try_make_interval(1, i) AS STRING)" ->string]
   ['try-make-timestamp [2026 10 :i 12 30 15.5] "try_make_timestamp(2026, 10, i, 12, 30, 15.5)"]
   ['try-make-timestamp-ltz [2026 10 :i 12 30 15.5] "try_make_timestamp_ltz(2026, 10, i, 12, 30, 15.5)"]
   ['try-make-timestamp-ntz [2026 10 :i 12 30 15.5] "try_make_timestamp_ntz(2026, 10, i, 12, 30, 15.5)"]
   ['try-mod [:l :z] "try_mod(l, z)"]
   ['try-multiply [:i :l] "try_multiply(i, l)"]
   ['try-parse-json [:j] "to_json(try_parse_json(j))" #(g/to-json %)]
   ['try-parse-url [:url (g/lit "HOST")] "try_parse_url(url, 'HOST')"]
   ['try-reflect [(g/lit "java.lang.Math") (g/lit "abs") :i] "try_reflect('java.lang.Math', 'abs', i)"]
   ['try-subtract [:i :l] "try_subtract(i, l)"]
   ['try-to-binary [:s (g/lit "hex")] "try_to_binary(s, 'hex')"]
   ['try-to-date [:s] "try_to_date(s)"]
   ['try-to-number [:s (g/lit "999")] "try_to_number(s, '999')"]
   ['try-to-time [(g/lit "12:34")] "CAST(try_to_time('12:34') AS STRING)" ->string]
   ['try-to-timestamp [:tss] "try_to_timestamp(tss)"]
   ['try-url-decode [:encoded] "try_url_decode(encoded)"]
   ['try-validate-utf8 [:s] "try_validate_utf8(s)"]
   ['try-variant-get [(delay (g/parse-json :j)) "$.a" "string"] "try_variant_get(parse_json(j), '$.a', 'string')"]
   ['typeof [:m] "typeof(m)"]
   ['ucase [:s] "ucase(s)"]
   ['uniform [1 10 42] "uniform(1, 10, 42)"]
   ['unix-date [:dt] "unix_date(dt)"]
   ['unix-micros [:ts] "unix_micros(ts)"]
   ['unix-millis [:ts] "unix_millis(ts)"]
   ['unix-seconds [:ts] "unix_seconds(ts)"]
   ['url-decode [:encoded] "url_decode(encoded)"]
   ['url-encode [:s] "url_encode(s)"]
   ['user [] "user()"]
   ['uuid [] "length(uuid())" #(g/length %)]
   ['validate-utf8 [:s] "validate_utf8(s)"]
   ['variant-get [(delay (g/parse-json :j)) "$.b[0]" "int"] "variant_get(parse_json(j), '$.b[0]', 'int')"]
   ['weekday [:dt] "weekday(dt)"]
   ['width-bucket [:d 0 10 5] "width_bucket(d, 0, 10, 5)"]
   ['xpath [:xml (g/lit "a/b/text()")] "xpath(xml, 'a/b/text()')"]
   ['xpath-boolean [:xml (g/lit "a/b")] "xpath_boolean(xml, 'a/b')"]
   ['xpath-double [:xml (g/lit "sum(a/b)")] "xpath_double(xml, 'sum(a/b)')"]
   ['xpath-float [:xml (g/lit "sum(a/b)")] "xpath_float(xml, 'sum(a/b)')"]
   ['xpath-int [:xml (g/lit "sum(a/b)")] "xpath_int(xml, 'sum(a/b)')"]
   ['xpath-long [:xml (g/lit "sum(a/b)")] "xpath_long(xml, 'sum(a/b)')"]
   ['xpath-number [:xml (g/lit "sum(a/b)")] "xpath_number(xml, 'sum(a/b)')"]
   ['xpath-short [:xml (g/lit "sum(a/b)")] "xpath_short(xml, 'sum(a/b)')"]
   ['xpath-string [:xml (g/lit "a/b")] "xpath_string(xml, 'a/b')"]
   ['zeroifnull [:n] "zeroifnull(n)"]
   ['schema-of-xml ["<a><b>1</b><b>2</b></a>"] "schema_of_xml('<a><b>1</b><b>2</b></a>')"]
   ['schema-of-variant [(delay (g/parse-json :j))] "schema_of_variant(parse_json(j))"]
   ['current-time [] "CAST(current_time() AS STRING)" ->string]
   ['time-bucket [(g/expr "INTERVAL '1' HOUR") :ts] "time_bucket(INTERVAL '1' HOUR, ts)"]])

(defn- kll [kind] (str "kll-sketch-agg-" kind))

(def ^:private aggregate-examples
  "[function args sql wrap], aggregated over the fixture."
  (concat
   [['any [:b] "any(b)"]
    ['any-value [:s true] "any_value(s, true)"]
    ['approx-percentile [:d 0.5 100] "approx_percentile(d, 0.5, 100)"]
    ['array-agg [:i] "array_sort(array_agg(i))" #(g/array-sort %)]
    ['bit-and [:i] "bit_and(i)"]
    ['bit-or [:i] "bit_or(i)"]
    ['bit-xor [:i] "bit_xor(i)"]
    ['bitmap-construct-agg [(g/bitmap-bit-position :i)]
     "bitmap_count(bitmap_construct_agg(bitmap_bit_position(i)))" #(g/bitmap-count %)]
    ['bitmap-count [(g/bitmap-construct-agg (g/bitmap-bit-position :i))]
     "bitmap_count(bitmap_construct_agg(bitmap_bit_position(i)))"]
    ['bool-and [:b] "bool_and(b)"]
    ['bool-or [:b] "bool_or(b)"]
    ['count-if [:b] "count_if(b)"]
    ['every [:b] "every(b)"]
    ['first-value [:s] "first_value(s)"]
    ['histogram-numeric [:d 3] "histogram_numeric(d, 3)"]
    ['hll-sketch-agg [:i 12] "hll_sketch_estimate(hll_sketch_agg(i, 12))" #(g/hll-sketch-estimate %)]
    ['hll-sketch-estimate [(g/hll-sketch-agg :i)] "hll_sketch_estimate(hll_sketch_agg(i))"]
    ['hll-union [(g/hll-sketch-agg :i) (g/hll-sketch-agg :l) true]
     "hll_sketch_estimate(hll_union(hll_sketch_agg(i), hll_sketch_agg(l), true))" #(g/hll-sketch-estimate %)]
    ['last-value [:s true] "last_value(s, true)"]
    ['listagg [:s] "listagg(s)"]
    ['listagg-distinct [:s (g/lit "|")] "listagg(DISTINCT s, '|')"]
    ['max-by [:s :i] "max_by(s, i)"]
    ['min-by [:s :i] "min_by(s, i)"]
    ['mode [:s] "mode(s)"]
    ['percentile [:d 0.5] "percentile(d, 0.5)"]
    ['percentile-approx [:d 0.5 100] "percentile_approx(d, 0.5, 100)"]
    ['product [:d] "aggregate(collect_list(d), 1D, (acc, x) -> acc * x)"]
    ['regr-avgx [:d :i] "regr_avgx(d, i)"]
    ['regr-avgy [:d :i] "regr_avgy(d, i)"]
    ['regr-count [:d :i] "regr_count(d, i)"]
    ['regr-intercept [:d :i] "regr_intercept(d, i)"]
    ['regr-r2 [:d :i] "regr_r2(d, i)"]
    ['regr-slope [:d :i] "regr_slope(d, i)"]
    ['regr-sxx [:d :i] "regr_sxx(d, i)"]
    ['regr-sxy [:d :i] "regr_sxy(d, i)"]
    ['regr-syy [:d :i] "regr_syy(d, i)"]
    ['schema-of-variant-agg [(delay (g/parse-json :j))] "schema_of_variant_agg(parse_json(j))"]
    ['some [:b] "some(b)"]
    ['string-agg [:s (g/lit "|")] "string_agg(s, '|')"]
    ['string-agg-distinct [:s] "string_agg(DISTINCT s)"]
    ['theta-sketch-agg [:i 12] "theta_sketch_estimate(theta_sketch_agg(i, 12))" #(g/theta-sketch-estimate %)]
    ['theta-sketch-estimate [(delay (g/theta-sketch-agg :i))] "theta_sketch_estimate(theta_sketch_agg(i))"]
    ['theta-union [(delay (g/theta-sketch-agg :i)) (delay (g/theta-sketch-agg :l))]
     "theta_sketch_estimate(theta_union(theta_sketch_agg(i), theta_sketch_agg(l)))" #(g/theta-sketch-estimate %)]
    ['theta-intersection [(delay (g/theta-sketch-agg :i)) (delay (g/theta-sketch-agg :l))]
     "theta_sketch_estimate(theta_intersection(theta_sketch_agg(i), theta_sketch_agg(l)))"
     #(g/theta-sketch-estimate %)]
    ['theta-difference [(delay (g/theta-sketch-agg :i)) (delay (g/theta-sketch-agg :l))]
     "theta_sketch_estimate(theta_difference(theta_sketch_agg(i), theta_sketch_agg(l)))"
     #(g/theta-sketch-estimate %)]
    ['try-avg [:d] "try_avg(d)"]
    ['try-sum [:l] "try_sum(l)"]]
   (for [[kind column] [["bigint" :l] ["double" :d] ["float" (g/cast :d "float")]]
         :let [col-sql (if (keyword? column) (name column) "CAST(d AS FLOAT)")
               agg-sym (symbol (kll kind))
               sketch  (str "kll_sketch_agg_" kind "(" col-sql ")")
               get-n   #(@(ns-resolve 'zero-one.geni.core (symbol (str "kll-sketch-get-n-" kind))) %)
               agg     #(@(ns-resolve 'zero-one.geni.core agg-sym) column)]
         row [[agg-sym [column] (str "kll_sketch_get_n_" kind "(" sketch ")") get-n]
              [(symbol (str "kll-sketch-get-n-" kind)) [(delay (agg))] (str "kll_sketch_get_n_" kind "(" sketch ")")]
              [(symbol (str "kll-sketch-get-quantile-" kind)) [(delay (agg)) 0.5]
               (str "kll_sketch_get_quantile_" kind "(" sketch ", 0.5)")]
              [(symbol (str "kll-sketch-get-rank-" kind)) [(delay (agg)) 2]
               (str "kll_sketch_get_rank_" kind "(" sketch ", 2)")]
              [(symbol (str "kll-sketch-merge-" kind)) [(delay (agg)) (delay (agg))]
               (str "kll_sketch_get_n_" kind "(kll_sketch_merge_" kind "(" sketch ", " sketch "))") get-n]
              [(symbol (str "kll-sketch-to-string-" kind)) [(delay (agg))]
               (str "kll_sketch_to_string_" kind "(" sketch ")")]]]
     row)
   (for [[kind summary] [["double" :d] ["integer" :i]]
         :let [summary-sql (name summary)
               sketch      (str "tuple_sketch_agg_" kind "(i, " summary-sql ")")
               theta       "theta_sketch_agg(i)"
               resolve-fn  #(deref (ns-resolve 'zero-one.geni.core (symbol (str % kind))))
               estimate    #((resolve-fn "tuple-sketch-estimate-") %)
               agg         #((resolve-fn "tuple-sketch-agg-") :i summary)
               estimate-of #(str "tuple_sketch_estimate_" kind "(" % ")")]
         row [[(symbol (str "tuple-sketch-agg-" kind)) [:i summary] (estimate-of sketch) estimate]
              [(symbol (str "tuple-sketch-estimate-" kind)) [(delay (agg))] (estimate-of sketch)]
              [(symbol (str "tuple-sketch-summary-" kind)) [(delay (agg))]
               (str "tuple_sketch_summary_" kind "(" sketch ")")]
              [(symbol (str "tuple-sketch-theta-" kind)) [(delay (agg))]
               (str "tuple_sketch_theta_" kind "(" sketch ")")]
              [(symbol (str "tuple-union-" kind)) [(delay (agg)) (delay (agg))]
               (estimate-of (str "tuple_union_" kind "(" sketch ", " sketch ")")) estimate]
              [(symbol (str "tuple-intersection-" kind)) [(delay (agg)) (delay (agg))]
               (estimate-of (str "tuple_intersection_" kind "(" sketch ", " sketch ")")) estimate]
              [(symbol (str "tuple-difference-" kind)) [(delay (agg)) (delay (agg))]
               (estimate-of (str "tuple_difference_" kind "(" sketch ", " sketch ")")) estimate]
              [(symbol (str "tuple-union-theta-" kind)) [(delay (agg)) (delay (g/theta-sketch-agg :i))]
               (estimate-of (str "tuple_union_theta_" kind "(" sketch ", " theta ")")) estimate]
              [(symbol (str "tuple-intersection-theta-" kind)) [(delay (agg)) (delay (g/theta-sketch-agg :i))]
               (estimate-of (str "tuple_intersection_theta_" kind "(" sketch ", " theta ")")) estimate]
              [(symbol (str "tuple-difference-theta-" kind)) [(delay (agg)) (delay (g/theta-sketch-agg :i))]
               (estimate-of (str "tuple_difference_theta_" kind "(" sketch ", " theta ")")) estimate]]]
     row)))

(def ^:private merge-agg-examples
  "[function args sql wrap sketches], aggregated over a sketch per group, from
  the SQL `sketches` over `range(1, 10)` grouped by `id % 3`, as `sk`."
  (concat
   [['bitmap-and-agg [:sk] "bitmap_count(bitmap_and_agg(sk))" #(g/bitmap-count %)
     "bitmap_construct_agg(bitmap_bit_position(id))"]
    ['bitmap-or-agg [:sk] "bitmap_count(bitmap_or_agg(sk))" #(g/bitmap-count %)
     "bitmap_construct_agg(bitmap_bit_position(id))"]
    ['hll-union-agg [:sk true] "hll_sketch_estimate(hll_union_agg(sk, true))" #(g/hll-sketch-estimate %)
     "hll_sketch_agg(id)"]
    ['theta-intersection-agg [:sk] "theta_sketch_estimate(theta_intersection_agg(sk))"
     #(g/theta-sketch-estimate %) "theta_sketch_agg(id)"]
    ['theta-union-agg [:sk] "theta_sketch_estimate(theta_union_agg(sk))" #(g/theta-sketch-estimate %)
     "theta_sketch_agg(id)"]]
   (for [kind ["bigint" "double" "float"]
         :let [column (if (= kind "bigint") "id" (str "CAST(id AS " (string/upper-case kind) ")"))]]
     [(symbol (str "kll-merge-agg-" kind)) [:sk] (str "kll_sketch_get_n_" kind "(kll_merge_agg_" kind "(sk))")
      #(@(ns-resolve 'zero-one.geni.core (symbol (str "kll-sketch-get-n-" kind))) %)
      (str "kll_sketch_agg_" kind "(" column ")")])
   (for [kind ["double" "integer"]
         [agg sql-agg] [["tuple-union-agg-" "tuple_union_agg_"]
                        ["tuple-intersection-agg-" "tuple_intersection_agg_"]]]
     [(symbol (str agg kind)) [:sk] (str "tuple_sketch_estimate_" kind "(" sql-agg kind "(sk))")
      #(@(ns-resolve 'zero-one.geni.core (symbol (str "tuple-sketch-estimate-" kind))) %)
      (str "tuple_sketch_agg_" kind "(id, " (if (= kind "double") "CAST(id AS DOUBLE)" "CAST(id AS INT)") ")")])))

(def ^:private window-examples
  "[function args sql], over the fixture ordered by `i`."
  [['lag [:d 1] "lag(d, 1) OVER (ORDER BY i)"]
   ['lead [:n 1 0 true] "lead(n, 1, 0) IGNORE NULLS OVER (ORDER BY i)"]
   ['nth-value [:s 2] "nth_value(s, 2) OVER (ORDER BY i)"]])

(def ^:private generator-examples
  "[function args sql], whose rows are selected from the fixture."
  [['explode-outer [:ys] "explode_outer(ys)"]
   ['inline [:structs] "inline(structs)"]
   ['inline-outer [:structs] "inline_outer(structs)"]
   ['json-tuple [:j "a" "b"] "json_tuple(j, 'a', 'b')"]
   ['posexplode-outer [:ys] "posexplode_outer(ys)"]
   ['stack [2 :i :s :n :t] "stack(2, i, s, n, t)"]])

(def ^:private partition-transforms
  "Functions for `write-to!`'s `:partitioned-by`, which Spark refuses in a
  query, as [function args]."
  [['bucket [4 :i]]
   ['days [:ts]]
   ['hours [:ts]]
   ['months [:ts]]
   ['years [:ts]]])

(def ^:private other-functions
  "Functions tested on their own, below."
  '#{raise-error session-window unwrap-udt window-time})

(defn- realise
  "The examples' arguments, with delayed columns forced, which are delayed so
  that a function's example only builds the columns it uses."
  [[sym args & more]]
  (into [sym (mapv #(if (delay? %) @% %) args)] more))

(defn- this-sparks? [[sym]]
  (let [since (:since (get @function-table/table sym))]
    (or (nil? since) (at-least? since))))

(defn- check-older-sparks
  "Checks that the examples' functions that need a newer Spark throw an error
  that says so."
  [examples]
  (doseq [[sym args] (remove this-sparks? examples)]
    (is (thrown-with-msg? ExceptionInfo #"needs Spark"
                          (apply @(ns-resolve 'zero-one.geni.core sym)
                                 (mapv #(if (delay? %) (g/lit 1) %) args)))
        (str sym))))

(defn- run-examples [examples run]
  (check-older-sparks examples)
  (doseq [row (filter this-sparks? examples)]
    (check-row (realise row) run)))

(defn- run-batch [rows query run]
  (let [built   (keep-indexed
                 (fn [i row]
                   (let [[sym args sql wrap] (realise row)]
                     (try
                       {:row  row
                        :geni [(keyword (str "geni-" i))
                               ((or wrap identity) (apply @(ns-resolve 'zero-one.geni.core sym) args))]
                        :sql  [(keyword (str "sql-" i)) (g/expr sql)]}
                       (catch Exception e
                         (is (nil? e) (str sym " threw " (ex-message e)))
                         nil))))
                 rows)
        columns (into {} (mapcat (juxt :geni :sql)) built)
        results (try (query columns) (catch Exception _ nil))]
    (if results
      (doseq [{:keys [row geni sql]} built]
        (is (= (comparable (map (first sql) results)) (comparable (map (first geni) results)))
            (str (first row) " " (pr-str (second row)) " vs " (nth row 2))))
      (doseq [{:keys [row]} built]
        (check-row (realise row) run)))))

(defn- run-batched
  "Runs the examples in queries of 50, through `query`, which takes a map of
  names to columns and returns the rows, and checks each Geni column against
  its SQL. When a query fails, it runs its examples one at a time with `run`,
  to say which. Fifty keeps Spark's planning quick, which slows down a lot
  for hundreds of columns."
  [examples query run]
  (check-older-sparks examples)
  (doseq [rows (partition-all 50 (filter this-sparks? examples))]
    (run-batch rows query run)))

(deftest every-function-has-an-example-test
  (is (= (set (keys @function-table/table))
         (set (concat (map first scalar-examples)
                      (map first aggregate-examples)
                      (map first merge-agg-examples)
                      (map first window-examples)
                      (map first generator-examples)
                      (map first partition-transforms)
                      other-functions)))))

(deftest scalar-functions-test
  ;; Spark 4.1 and 4.2 have the TIME type behind a flag.
  (let [time-type? (at-least? "4.1")]
    (when time-type? (g/conf-set! "spark.sql.timeType.enabled" true))
    (try
      (run-batched scalar-examples #(g/collect (g/select (fixture) %)) (select-run (fixture)))
      (finally
        (when time-type? (g/conf-unset! "spark.sql.timeType.enabled"))))))

(deftest aggregate-functions-test
  (run-batched aggregate-examples #(g/collect (g/agg (fixture) %)) (agg-run (fixture))))

(deftest merge-aggregate-functions-test
  (doseq [[sym args sql wrap sketches] merge-agg-examples
          :let [since (:since (get @function-table/table sym))]]
    (if (and since (not (at-least? since)))
      (is (thrown-with-msg? ExceptionInfo #"needs Spark"
                            (apply @(ns-resolve 'zero-one.geni.core sym) args))
          (str sym))
      (check-row [sym args sql wrap]
                 (agg-run (g/sql @spark (str "SELECT " sketches " AS sk FROM range(1, 10) GROUP BY id % 3")))))))

(deftest window-functions-test
  (let [window (g/window {:order-by [:i]})]
    (run-examples window-examples
                  (fn [geni sql]
                    (let [rows (g/collect (g/select (fixture) {:geni (g/over geni window) :sql sql}))]
                      [(map :geni rows) (map :sql rows)])))))

(deftest generator-functions-test
  (run-examples generator-examples
                (fn [geni sql]
                  [(sort-by str (g/collect-vals (g/select (fixture) geni)))
                   (sort-by str (g/collect-vals (g/select (fixture) sql)))])))

(deftest partition-transforms-test
  (doseq [[sym args] partition-transforms]
    (is (instance? Column (apply @(ns-resolve 'zero-one.geni.core sym) args)) (str sym))))

(deftest raise-error-test
  (without-task-error-logs
   #(is (thrown? Exception (g/collect (g/select (fixture) (g/raise-error (g/lit "boom"))))))))

(deftest session-and-window-time-test
  (let [counts (fn [grouping]
                 (-> (fixture) (g/group-by grouping) (g/agg {:n (g/count "*")}) (g/order-by :n) (g/collect-col :n)))]
    (is (= (counts (g/expr "session_window(ts, '90 minutes')"))
           (counts (g/session-window :ts "90 minutes")))))
  (let [windows (-> (fixture) (g/group-by (g/time-window :ts "1 hour")) (g/agg {:n (g/count "*")}))]
    (is (= (g/collect-vals (g/select windows (g/expr "window_time(window)")))
           (g/collect-vals (g/select windows (g/window-time :window)))))))

(deftest ^:classic unwrap-udt-test
  (let [vectors (g/table->dataset @spark [[(g/dense 1.0 2.0)]] [:v])]
    (is (= [{:type 1 :size nil :indices nil :values [1.0 2.0]}]
           (g/collect-col (g/select vectors {:u (g/unwrap-udt :v)}) :u)))))

;;;; Coverage

(def ^:private covered-elsewhere
  "Spark functions that Geni has under another name."
  {"approxCountDistinct"       'approx-count-distinct
   "bitwiseNOT"                'bitwise-not
   "callUDF"                   'call-udf
   "column"                    'col
   "countDistinct"             'count-distinct
   "monotonicallyIncreasingId" 'monotonically-increasing-id
   "replace"                   'replace-substring
   "shiftLeft"                 'shift-left
   "shiftRight"                'shift-right
   "shiftRightUnsigned"        'shift-right-unsigned
   "sumDistinct"               'sum-distinct
   "toDegrees"                 'degrees
   "toRadians"                 'radians
   "typedLit"                  'lit
   "typedlit"                  'lit})

(def ^:private out-of-scope
  "Spark functions that Geni leaves out, and why."
  {"udaf"    "Scala Aggregators, which take an Encoder and a TypeTag"
   "version" "g/version is the session's Spark version"})

(defn- kebab-case [spark-name]
  (-> spark-name
      (string/replace #"([a-z0-9])([A-Z])" "$1_$2")
      string/lower-case
      (string/replace "_" "-")))

(defn- spark-function-names []
  (->> (.getMethods functions)
       (filter #(Modifier/isStatic (.getModifiers ^Method %)))
       (map #(.getName ^Method %))
       set))

(deftest coverage-test
  (let [spark-names (spark-function-names)
        geni-names  (set (map str (keys (ns-publics 'zero-one.geni.core))))]
    (testing "the table's functions are this Spark's, from their versions"
      (doseq [[sym {:keys [spark since]}] @function-table/table
              :when (or (nil? since) (at-least? since))]
        (is (spark-names spark) (str sym " has no Spark function " spark))))
    (testing "the other names exist"
      (doseq [[spark-name sym] covered-elsewhere]
        (is (geni-names (str sym)) spark-name)))
    (testing "Spark's functions that Geni lacks, which a newer Spark brings: reported, not failed"
      (let [missing (sort (remove #(or (out-of-scope %)
                                       (geni-names (str (get covered-elsewhere % (kebab-case %)))))
                                  spark-names))]
        ;; On stderr, which the runner leaves alone, so that the canary's log shows it.
        (when (seq missing)
          (binding [*out* *err*]
            (println "Spark functions that Geni doesn't have yet:" (string/join ", " missing))))))))
