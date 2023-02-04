namespace Cactus.ferate

open System
open System.Net.Http
open System.Threading.Tasks
open System.Text
open System.IO

module Ferate =
  let now () = DateOnly.FromDateTime DateTime.Now

  [<Literal>]
  let private ep = "https://www.mizuhobank.co.jp/market/csv/quote.csv"
  let private client = new HttpClient()
  let private enc =
    Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
    Encoding.GetEncoding "Shift_JIS"
  let mutable private last'updated =  DateOnly.FromDateTime DateTime.MinValue
  let mutable private historical'data: string[] = [||]


  let monitor = Object()

  let fetch'historical'data () = 
    lock monitor (fun () ->
      if last'updated < now() then
        task { 
          let! r = client.GetAsync ep
          use! s = r.Content.ReadAsStreamAsync()
          use ms = new MemoryStream()
          do! s.CopyToAsync(ms)
          let csv = enc.GetString(ms.ToArray())
          historical'data <- (csv.Split "\n")[2..]
          historical'data[0] <- $"DATE{historical'data[0]}"
          // let! res = client.GetStringAsync ep
          // historical'data <- res.Split "\r\n"
        }
        |> Task.WaitAll
      historical'data
    )