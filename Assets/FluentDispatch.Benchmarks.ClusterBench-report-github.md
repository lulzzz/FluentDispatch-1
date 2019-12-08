``` ini

BenchmarkDotNet=v0.12.0, OS=Windows 10.0.18363
Intel Core i7-8565U CPU 1.80GHz (Whiskey Lake), 1 CPU, 8 logical and 4 physical cores
.NET Core SDK=3.1.100
  [Host]     : .NET Core 3.1.0 (CoreCLR 4.700.19.56402, CoreFX 4.700.19.56404), X64 RyuJIT
  DefaultJob : .NET Core 3.1.0 (CoreCLR 4.700.19.56402, CoreFX 4.700.19.56404), X64 RyuJIT


```
|                           Method |    Mean |    Error |   StdDev |  Median |     Gen 0 |     Gen 1 | Gen 2 | Allocated |
|--------------------------------- |--------:|---------:|---------:|--------:|----------:|----------:|------:|----------:|
| &#39;Fire-And-Forget FluentDispatch&#39; | 2.360 s | 0.0466 s | 0.1014 s | 2.389 s | 8000.0000 | 1000.0000 |     - |  35.57 MB |
|       &#39;Fire-And-Forget Task.Run&#39; | 2.381 s | 0.0481 s | 0.1153 s | 2.342 s |         - |         - |     - |   3.12 MB |
