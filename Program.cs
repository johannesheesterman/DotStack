using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Threading;
using Microsoft.Diagnostics.NETCore.Client;
using Microsoft.Diagnostics.Tracing;
using Microsoft.Diagnostics.Tracing.Etlx;
using Microsoft.Diagnostics.Tracing.EventPipe;
using Microsoft.Diagnostics.Tracing.Parsers;
using Spectre.Console;

class Program
{
    private static readonly WeightedCallGraph Graph = new();
    private static double LastAvgSampleIntervalMs = 0;
    private static string[] Filters = Array.Empty<string>();

    static int Main(string[] args)
    {
        if (args.Length < 1 || !int.TryParse(args[0], out var pid))
        {
            Console.WriteLine("Usage: HotMethodsCumulative <pid> [windowSec=2] [topN=50] [filters=foo,bar]");
            return 1;
        }
        int windowSec = args.Length >= 2 && int.TryParse(args[1], out var w) ? Math.Max(1, w) : 2;
        int topN = args.Length >= 3 && int.TryParse(args[2], out var t) ? Math.Max(1, t) : 50;
        if (args.Length >= 4 && !string.IsNullOrWhiteSpace(args[3]))
            Filters = args[3].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);


        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        while (!cts.IsCancellationRequested)
        {
            string? trace = null, etlx = null;
            try
            {
                trace = CollectWindow(pid, windowSec, requestRundown: true);

                var window = AggregateWindow(trace, out etlx);

                if (window.AvgSampleIntervalMs > 0) LastAvgSampleIntervalMs = window.AvgSampleIntervalMs;
                var intervalForWindow = LastAvgSampleIntervalMs > 0 ? LastAvgSampleIntervalMs : 1.0; 

                IEnumerable<CallGraphNode> windowNodes = window.Nodes.Values;
                if (Filters.Length > 0)
                    windowNodes = windowNodes.Where(n => MatchesNameOnly(n.Name));
                foreach (var node in windowNodes)
                    Graph.AddNodeSamples(node.Name, node.InclusiveSamplesCount, intervalForWindow);

                IEnumerable<CallGraphEdge> windowEdges = window.Edges.Values;
                if (Filters.Length > 0)
                    windowEdges = windowEdges.Where(e => MatchesNameOnly(e.Caller) || MatchesNameOnly(e.Callee));
                foreach (var edge in windowEdges)
                    Graph.AddEdgeSamples(edge.Caller, edge.Callee, edge.SamplesCount, intervalForWindow);

                RenderSpectre(pid, windowSec, topN, window);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[warn] {ex.GetType().Name}: {ex.Message}");
            }
            finally
            {
                TryDelete(trace);
                TryDelete(etlx);
            }

            while (Console.KeyAvailable)
            {
                var k = Console.ReadKey(intercept: true);
                if (k.Key == ConsoleKey.C)
                {
                    Graph.Clear();
                    LastAvgSampleIntervalMs = 0;
                    AnsiConsole.Clear();
                }
            }
        }
        return 0;
    }

    private static void RenderSpectre(int pid, int windowSec, int topN, WindowAggregationResult window)
    {
        AnsiConsole.Cursor.Hide();
        AnsiConsole.Clear();
        var filterEsc = Escape(string.Join(", ", Filters));
        var header = new Rule($"PID={pid} | Window={windowSec}s | Top={topN} | Filters={filterEsc}") { Justification = Justify.Left };
        AnsiConsole.Write(header);
        AnsiConsole.MarkupLine($"[dim]Updated @ {DateTime.Now:HH:mm:ss}[/]");

        if (window.MatchedSamples == 0)
        {
            AnsiConsole.MarkupLine($"[yellow]No samples matched filters[/] {filterEsc}");
            if (window.Examples.Count > 0)
            {
                var eg = new Table().Border(TableBorder.Rounded).AddColumn("Example frames (unfiltered)");
                foreach (var ex in window.Examples.Take(8))
                    eg.AddRow(Escape(ex));
                AnsiConsole.Write(eg);
            }
            return;
        }

        AnsiConsole.MarkupLine($"[grey](inclusive: parent counts include children; interval≈{LastAvgSampleIntervalMs:F3} ms)[/]");

        var nodeTable = new Table().Border(TableBorder.SimpleHeavy);
        nodeTable.AddColumn("Samples");
        nodeTable.AddColumn("CPU-ms");
        nodeTable.AddColumn("Percent");
        nodeTable.AddColumn("Method (inclusive)");

        foreach (var node in Graph.Nodes.Values.OrderByDescending(n => n.InclusiveSamplesCount).Take(topN))
        {
            double pct = (Graph.TotalCpuMsSum > 0) ? (100.0 * node.CpuMs / Graph.TotalCpuMsSum) : 0.0;
            nodeTable.AddRow(node.InclusiveSamplesCount.ToString(), node.CpuMs.ToString("F1"), pct.ToString("F1") + "%", Escape(node.Name));
        }
        AnsiConsole.Write(nodeTable);

        AnsiConsole.MarkupLine($"[dim]Window stats: matched {window.MatchedSamples} of {window.TotalSamples} samples[/]");

        AnsiConsole.MarkupLine("[dim]Press 'C' to clear cumulative data. Ctrl+C to exit.[/]");
        AnsiConsole.Cursor.Show();
    }

    private static string Escape(string? v)
        => string.IsNullOrEmpty(v) ? string.Empty : v.Replace("[", "[[").Replace("]", "]]");

    static string CollectWindow(int pid, int seconds, bool requestRundown)
    {
        var providers = new List<EventPipeProvider>
        {
            new EventPipeProvider("Microsoft-DotNETCore-SampleProfiler", EventLevel.Informational),
            new EventPipeProvider("Microsoft-DotNETRuntime", EventLevel.Informational,
                (long)ClrTraceEventParser.Keywords.Loader |
                (long)ClrTraceEventParser.Keywords.Jit)
        };

        var file = Path.Combine(Path.GetTempPath(),
            $"hot-{DateTime.UtcNow:yyyyMMdd_HHmmss_fff}-{Environment.ProcessId}.nettrace");

        var client = new DiagnosticsClient(pid);
        using var session = client.StartEventPipeSession(
            providers, requestRundown: requestRundown, circularBufferMB: 512);

        using var fs = File.Create(file);
        var copyTask = session.EventStream.CopyToAsync(fs);

        Thread.Sleep(TimeSpan.FromSeconds(seconds));
        session.Stop();
        copyTask.Wait();

        return file;
    }

    static WindowAggregationResult AggregateWindow(string nettrace, out string etlxPath)
    {
        etlxPath = TraceLog.CreateFromEventPipeDataFile(nettrace);
        using var log = new TraceLog(etlxPath);
        var src = log.Events.GetSource();

        var counts = new Dictionary<string, CallGraphNode>(capacity: 1 << 14);
        var edgeCounts = new Dictionary<(string Caller, string Callee), CallGraphEdge>(capacity: 1 << 15);
        int matched = 0, total = 0;
        var lastPerThread = new Dictionary<int, double>();
        double intervalSum = 0; long intervalCount = 0;

        var examples = new HashSet<string>(StringComparer.Ordinal);
        var sp = new SampleProfilerTraceEventParser(src);

        sp.ThreadSample += e =>
        {
            var cs = e.CallStack();
            if (cs == null) return;
            total++;
            bool sampleHasFilteredFrame = false;
            var frame = cs;
            string? calleeBelow = null;
            while (frame != null)
            {
                var method = frame.CodeAddress?.Method;
                var full = method?.FullMethodName;
                var module = method?.MethodModuleFile?.Name;
                var chosen = full ?? module ?? frame.ToString();
                if (!string.IsNullOrEmpty(chosen))
                {
                    if (counts.TryGetValue(chosen, out var existingNode))
                        existingNode.InclusiveSamplesCount++;
                    else
                        counts[chosen] = new CallGraphNode(chosen, 1);
                    if (Matches(full, module))
                        sampleHasFilteredFrame = true;
                    if (examples.Count < 16)
                        examples.Add(chosen);
                    if (!string.IsNullOrEmpty(calleeBelow))
                    {
                        var edgeKey = (Caller: chosen, Callee: calleeBelow);
                        if (edgeCounts.TryGetValue(edgeKey, out var edgeObj))
                            edgeObj.SamplesCount++;
                        else
                            edgeCounts[edgeKey] = new CallGraphEdge(edgeKey.Caller, edgeKey.Callee, 1);
                    }
                }
                calleeBelow = chosen;
                frame = frame.Caller;
            }
            if (sampleHasFilteredFrame) matched++;

            var tid = e.ThreadID;
            double ts = e.TimeStampRelativeMSec;
            if (lastPerThread.TryGetValue(tid, out var prevTs))
            {
                var delta = ts - prevTs;
                if (delta > 0 && delta < 500) // ignore outliers / large gaps
                {
                    intervalSum += delta;
                    intervalCount++;
                }
            }
            lastPerThread[tid] = ts;
        };

        src.Process();
        double avgIntervalMs = intervalCount > 0 ? intervalSum / intervalCount : 0;
        return new WindowAggregationResult(counts, edgeCounts, matched, total, examples.ToList(), avgIntervalMs);
    }

    static bool Matches(string? fullMethodName, string? modulePath)
    {
        foreach (var f in Filters)
        {
            if (string.IsNullOrEmpty(f)) continue;
            if (!string.IsNullOrEmpty(fullMethodName) &&
                fullMethodName.IndexOf(f + ".", StringComparison.OrdinalIgnoreCase) >= 0)
                return true;

            if (!string.IsNullOrEmpty(modulePath))
            {
                var file = Path.GetFileName(modulePath);
                if (modulePath.IndexOf(f, StringComparison.OrdinalIgnoreCase) >= 0 ||
                    (!string.IsNullOrEmpty(file) && file.IndexOf(f, StringComparison.OrdinalIgnoreCase) >= 0))
                    return true;
            }
        }
        return false;
    }

    static bool MatchesNameOnly(string name)
    {
        if (Filters.Length == 0) return true;
        foreach (var f in Filters)
        {
            if (string.IsNullOrEmpty(f)) continue;
            if (name.IndexOf(f, StringComparison.OrdinalIgnoreCase) >= 0)
                return true;
        }
        return false;
    }

    static string Truncate(string? s, int max)
    {
        if (string.IsNullOrEmpty(s)) return string.Empty;
        return s.Length <= max ? s : s.Substring(0, Math.Max(0, max - 1)) + "…";
    }

    static void TryDelete(string? path)
    {
        try { if (!string.IsNullOrEmpty(path) && File.Exists(path)) File.Delete(path); }
        catch { /* ignore */ }
    }
}

internal sealed class CallGraphNode
{
    public string Name { get; }
    public long InclusiveSamplesCount { get; set; }
    public double CpuMs { get; set; }
    public CallGraphNode(string name, long samples)
    {
        Name = name;
        InclusiveSamplesCount = samples;
    }
}

internal sealed class CallGraphEdge
{
    public string Caller { get; }
    public string Callee { get; }
    public long SamplesCount { get; set; }
    public double CpuMs { get; set; }
    public CallGraphEdge(string caller, string callee, long samples)
    {
        Caller = caller;
        Callee = callee;
        SamplesCount = samples;
    }
}

internal sealed class WindowAggregationResult
{
    public Dictionary<string, CallGraphNode> Nodes { get; }
    public Dictionary<(string Caller, string Callee), CallGraphEdge> Edges { get; }
    public int MatchedSamples { get; }
    public int TotalSamples { get; }
    public List<string> Examples { get; }
    public double AvgSampleIntervalMs { get; }
    public WindowAggregationResult(Dictionary<string, CallGraphNode> nodes,
        Dictionary<(string Caller, string Callee), CallGraphEdge> edges,
        int matched, int total, List<string> examples, double avgMs)
    {
        Nodes = nodes;
        Edges = edges;
        MatchedSamples = matched;
        TotalSamples = total;
        Examples = examples;
        AvgSampleIntervalMs = avgMs;
    }
}

internal sealed class WeightedCallGraph
{
    public Dictionary<string, CallGraphNode> Nodes { get; } = new(capacity: 1 << 14);
    public Dictionary<(string Caller, string Callee), CallGraphEdge> Edges { get; } = new(capacity: 1 << 15);
    public double TotalCpuMsSum { get; private set; }
    public double TotalEdgeCpuMsSum { get; private set; }

    public void AddNodeSamples(string name, long samples, double intervalMs)
    {
        if (!Nodes.TryGetValue(name, out var node))
        {
            node = new CallGraphNode(name, 0);
            Nodes[name] = node;
        }
        node.InclusiveSamplesCount += samples;
        double addMs = samples * intervalMs;
        node.CpuMs += addMs;
        TotalCpuMsSum += addMs;
    }

    public void AddEdgeSamples(string caller, string callee, long samples, double intervalMs)
    {
        var key = (Caller: caller, Callee: callee);
        if (!Edges.TryGetValue(key, out var edge))
        {
            edge = new CallGraphEdge(caller, callee, 0);
            Edges[key] = edge;
        }
        edge.SamplesCount += samples;
        double addMs = samples * intervalMs;
        edge.CpuMs += addMs;
        TotalEdgeCpuMsSum += addMs;
    }

    public void Clear()
    {
        Nodes.Clear();
        Edges.Clear();
        TotalCpuMsSum = 0;
        TotalEdgeCpuMsSum = 0;
    }
}
