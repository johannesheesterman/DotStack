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

        Console.WriteLine($"PID={pid}, window={windowSec}s, topN={topN}, filters=[{string.Join(", ", Filters)}]");
        Console.WriteLine("Press Ctrl+C to stop. Press 'C' to clear cumulative data.");

        TryInitFullScreen();

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

                var lines = new List<string>();
                lines.Add("");
                lines.Add($"== {DateTime.Now:HH:mm:ss} | cumulative (processed last {windowSec}s) ==");
                if (window.MatchedSamples == 0)
                {
                    lines.Add($"(no samples matched namespaces/modules [{string.Join(", ", Filters)}])");
                    if (window.Examples.Count > 0)
                    {
                        lines.Add("example frames observed (unfiltered):");
                        foreach (var ex in window.Examples.Take(8))
                            lines.Add("  - " + ex);
                    }
                }
                else
                {
                    int width = Console.WindowWidth > 0 ? Console.WindowWidth : 120;
                    lines.Add($"(inclusive: parent counts/time include children; interval≈{LastAvgSampleIntervalMs:F3} ms)");
                    lines.Add($"{"Samples",9} {"CPU-ms",12} {"Percent",8}  Method (inclusive)");
                    foreach (var node in Graph.Nodes.Values.OrderByDescending(n => n.InclusiveSamplesCount).Take(topN))
                    {
                        double pct = (Graph.TotalCpuMsSum > 0) ? (100.0 * node.CpuMs / Graph.TotalCpuMsSum) : 0.0;
                        lines.Add($"{node.InclusiveSamplesCount,9} {node.CpuMs,12:F1} {pct,7:F1}%  {Truncate(node.Name, width - 37)}");
                    }
                    lines.Add($"-- window stats: matched {window.MatchedSamples} of {window.TotalSamples} samples (this window)");

                    if (Graph.Edges.Count > 0)
                    {
                        lines.Add("");
                        lines.Add("Top call edges (caller -> callee), inclusive samples");
                        lines.Add($"{"Samples",9} {"CPU-ms",12} {"Percent",8}  Edge");
                        foreach (var edge in Graph.Edges.Values.OrderByDescending(e => e.SamplesCount).Take(Math.Min(topN, 40)))
                        {
                            var edgeLabel = $"{Truncate(edge.Caller, (width - 40) / 2)} -> {Truncate(edge.Callee, (width - 40) / 2)}";
                            double pct = (Graph.TotalEdgeCpuMsSum > 0) ? (100.0 * edge.CpuMs / Graph.TotalEdgeCpuMsSum) : 0.0;
                            lines.Add($"{edge.SamplesCount,9} {edge.CpuMs,12:F1} {pct,7:F1}%  {Truncate(edgeLabel, width - 37)}");
                        }
                    }
                }
                BufferedRender(lines);
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
                    ForceFullRedraw();
                }
            }
        }
        return 0;
    }

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

    private static int _lastLineCount = 0;
    private static bool _fullScreenInitialized = false;

    private static void TryInitFullScreen()
    {
        if (_fullScreenInitialized) return;
        try
        {
            Console.Write("\x1b[2J\x1b[H");
            _fullScreenInitialized = true;
        }
        catch
        {
            try { Console.Clear(); _fullScreenInitialized = true; } catch { }
        }
    }

    private static void BufferedRender(List<string> lines)
    {
        int width;
        try { width = Console.WindowWidth > 0 ? Console.WindowWidth : 120; }
        catch { width = 120; }

        try { Console.SetCursorPosition(0, 0); } catch { TryInitFullScreen(); }

        Console.Write("\x1b[?25l");
        foreach (var line in lines)
        {
            var txt = line ?? string.Empty;
            if (txt.Length > width - 1)
                txt = txt.Substring(0, Math.Max(0, width - 2)) + '…';
            if (txt.Length < width - 1)
                txt = txt + new string(' ', width - 1 - txt.Length);
            Console.Write(txt);
            Console.WriteLine();
        }
        for (int i = lines.Count; i < _lastLineCount; i++)
        {
            Console.Write(new string(' ', Math.Max(1, width - 1)));
            Console.WriteLine();
        }
        _lastLineCount = lines.Count;
        try { Console.SetCursorPosition(0, 0); } catch { }
        Console.Write("\x1b[?25h");
    }

    private static void ForceFullRedraw()
    {
        _lastLineCount = 0;
        _fullScreenInitialized = false;
        TryInitFullScreen();
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
