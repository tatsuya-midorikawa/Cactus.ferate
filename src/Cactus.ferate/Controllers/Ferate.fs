namespace Cactus.ferate.Controllers

open System
open System.Collections.Generic
open System.Linq
open System.Threading.Tasks
open Microsoft.AspNetCore.Mvc
open Microsoft.Extensions.Logging
open Cactus.ferate

[<ApiController>]
[<Route("[controller]")>]
type FerateController (logger : ILogger<_>) =
  inherit ControllerBase()

  [<HttpGet>]
  member _.Get() =
    let csv = Ferate.fetch'historical'data()
    csv
