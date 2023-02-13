namespace Cactus.ferate

open System
open System.Net.Http
open System.Threading.Tasks
open System.Text
open System.IO
open System.Text.Json.Serialization
open System.Diagnostics


type Ferate = {
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] DATE: DateOnly option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] USD: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] GBP: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] EUR: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] CAD: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] CHF: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] SEK: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] DKK: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] NOK: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] AUD: decimal option

  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] NZD: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] ZAR: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] BHD: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] IDR: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] CNY: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] HKD: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] INR: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] MYR: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] PHP: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] SGD: decimal option
  
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] KRW: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] THB: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] KWD: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] SAR: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] AED: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] MKN: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] PGK: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] HUF: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] CZK: decimal option
  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] TRY: decimal option

  [<JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)>] PLN: decimal option
}
with 
  static member none = 
    { DATE = None; USD = None; GBP = None; EUR = None; CAD = None; CHF  =  None;  SEK = None; DKK = None; NOK = None; AUD = None;
      NZD  =  None;  ZAR = None; BHD = None; IDR = None; CNY = None; HKD  =  None;  INR = None; MYR = None; PHP = None; SGD = None;
      KRW  =  None;  THB = None; KWD = None; SAR = None; AED = None; MKN  =  None;  PGK = None; HUF = None; CZK = None; PLN = None;
      TRY  =  None;  }

module Ferate =
  let today () = DateOnly.FromDateTime DateTime.Now

  [<Literal>]
  let private ep = "https://www.mizuhobank.co.jp/market/csv/quote.csv"
  let private client = new HttpClient()
  let private enc =
    Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
    Encoding.GetEncoding "Shift_JIS"
  let mutable private last_updated =  DateOnly.FromDateTime DateTime.MinValue
  let mutable private raw_historical_data: string[] = [||]
  let mutable private historical_data: Ferate[] = [||]
  let private dateonly'parse (s: string) = match System.DateOnly.TryParse s with true, v -> Some v | _ -> None
  let private decimal'parse (s: string) = match System.Decimal.TryParse s with true, v -> Some v | _ -> None

  let private monitor = Object()
  let private idx = Map[ 
    ("DATE", 0); ("USD",  1); ("GBP",  2); ("EUR",  3); ("CAD",  4); ("CHF",  5); ("SEK",  6); ("DKK",  7); ("NOK",  8); ("AUD",  9);
    ("NZD", 10); ("ZAR", 11); ("BHD", 12); ("IDR", 13); ("CNY", 14); ("HKD", 15); ("INR", 16); ("MYR", 17); ("PHP", 18); ("SGD", 19);
    ("KRW", 20); ("THB", 21); ("KWD", 22); ("SAR", 23); ("AED", 24); ("MKN", 25); ("PGK", 26); ("HUF", 27); ("CZK", 28); ("PLN", 29);
    ("TRY", 30); ]
    
  let private build (xs: string[]) = 
    { DATE = dateonly'parse xs[idx["DATE"]]; USD = decimal'parse xs[idx["USD"]]; GBP = decimal'parse xs[idx["GBP"]]; EUR = decimal'parse xs[idx["EUR"]]; CAD = decimal'parse xs[idx["CAD"]]
      CHF  =  decimal'parse xs[idx["CHF"]];  SEK = decimal'parse xs[idx["SEK"]]; DKK = decimal'parse xs[idx["DKK"]]; NOK = decimal'parse xs[idx["NOK"]]; AUD = decimal'parse xs[idx["AUD"]]
      NZD  =  decimal'parse xs[idx["NZD"]];  ZAR = decimal'parse xs[idx["ZAR"]]; BHD = decimal'parse xs[idx["BHD"]]; IDR = decimal'parse xs[idx["IDR"]]; CNY = decimal'parse xs[idx["CNY"]]
      HKD  =  decimal'parse xs[idx["HKD"]];  INR = decimal'parse xs[idx["INR"]]; MYR = decimal'parse xs[idx["MYR"]]; PHP = decimal'parse xs[idx["PHP"]]; SGD = decimal'parse xs[idx["SGD"]]
      KRW  =  decimal'parse xs[idx["KRW"]];  THB = decimal'parse xs[idx["THB"]]; KWD = decimal'parse xs[idx["KWD"]]; SAR = decimal'parse xs[idx["SAR"]]; AED = decimal'parse xs[idx["AED"]]
      MKN  =  decimal'parse xs[idx["MKN"]];  PGK = decimal'parse xs[idx["PGK"]]; HUF = decimal'parse xs[idx["HUF"]]; CZK = decimal'parse xs[idx["CZK"]]; PLN = decimal'parse xs[idx["PLN"]]
      TRY  =  decimal'parse xs[idx["TRY"]];  }

  // TODO
  let private build'by (xs: string[], keys: string[]) = 
    { DATE = dateonly'parse xs[idx["DATE"]]; USD = decimal'parse xs[idx["USD"]]; GBP = decimal'parse xs[idx["GBP"]]; EUR = decimal'parse xs[idx["EUR"]]; CAD = decimal'parse xs[idx["CAD"]]
      CHF  =  decimal'parse xs[idx["CHF"]];  SEK = decimal'parse xs[idx["SEK"]]; DKK = decimal'parse xs[idx["DKK"]]; NOK = decimal'parse xs[idx["NOK"]]; AUD = decimal'parse xs[idx["AUD"]]
      NZD  =  decimal'parse xs[idx["NZD"]];  ZAR = decimal'parse xs[idx["ZAR"]]; BHD = decimal'parse xs[idx["BHD"]]; IDR = decimal'parse xs[idx["IDR"]]; CNY = decimal'parse xs[idx["CNY"]]
      HKD  =  decimal'parse xs[idx["HKD"]];  INR = decimal'parse xs[idx["INR"]]; MYR = decimal'parse xs[idx["MYR"]]; PHP = decimal'parse xs[idx["PHP"]]; SGD = decimal'parse xs[idx["SGD"]]
      KRW  =  decimal'parse xs[idx["KRW"]];  THB = decimal'parse xs[idx["THB"]]; KWD = decimal'parse xs[idx["KWD"]]; SAR = decimal'parse xs[idx["SAR"]]; AED = decimal'parse xs[idx["AED"]]
      MKN  =  decimal'parse xs[idx["MKN"]];  PGK = decimal'parse xs[idx["PGK"]]; HUF = decimal'parse xs[idx["HUF"]]; CZK = decimal'parse xs[idx["CZK"]]; PLN = decimal'parse xs[idx["PLN"]]
      TRY  =  decimal'parse xs[idx["TRY"]];  }

  let private fetch'historical'data () = 
    lock monitor (fun () ->
      if last_updated < today() then
        task { 
          let! r = client.GetAsync ep
          use! s = r.Content.ReadAsStreamAsync()
          use ms = new MemoryStream()
          do! s.CopyToAsync(ms)
          let csv = enc.GetString(ms.ToArray()).Split("\n")
          // Header を読み飛ばすため, 3 から開始.
          // 最後の不要な改行分を飛ばすため, 長さ-2 で終了.
          raw_historical_data <- csv[3..(csv.Length-2)]
          historical_data <-
            raw_historical_data
            |> Array.map (fun d ->
              let xs = d.Split ','
              try build xs with e ->  Debug.WriteLine $"### {e.Message}"; Ferate.none )
        }
        |> Task.WaitAll
        last_updated <- today()
      historical_data
    )
  
  let get'historical'data () =
    if last_updated < today() 
    then fetch'historical'data()
    else historical_data