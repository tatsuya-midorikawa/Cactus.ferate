namespace Cactus.ferate

open System
open System.Net.Http
open System.Threading.Tasks
open System.Text
open System.IO

type Ferate = {
  date: DateOnly option
  USD: decimal option
  GBP: decimal option
}

module Ferate =
  let now () = DateOnly.FromDateTime DateTime.Now

  [<Literal>]
  let private ep = "https://www.mizuhobank.co.jp/market/csv/quote.csv"
  let private client = new HttpClient()
  let private enc =
    Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
    Encoding.GetEncoding "Shift_JIS"
  let mutable private last_updated =  DateOnly.FromDateTime DateTime.MinValue
  let mutable private raw_historical_data: string[] = [||]
  let mutable private historical_data: Ferate[] = [||]
  let private date'parse (s: string) = match System.DateOnly.TryParse s with true, v -> Some v | _ -> None
  let private decimal'parse (s: string) = match System.Decimal.TryParse s with true, v -> Some v | _ -> None

  let monitor = Object()

  let private fetch'historical'data () = 
    lock monitor (fun () ->
      if last_updated < now() then
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
              try
                { date = date'parse xs[0]; USD = decimal'parse xs[1]; GBP = decimal'parse xs[2] }
              with 
                | e -> System.Diagnostics.Debug.WriteLine $"### {e.Message}"; { date = None; USD = None; GBP = None } )
        }
        |> Task.WaitAll
        last_updated <- now()
      historical_data
    )
  
  let get'historical'data () =
    if last_updated < now() 
    then fetch'historical'data()
    else historical_data