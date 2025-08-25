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
    private static readonly object _lock = new();
    private static volatile List<DisplayRow> _rows = new();
    private static volatile bool _dirty = true;
    private static volatile ViewMode _viewMode = ViewMode.BottomUp;

    static int Main(string[] args)
    {
        if (args.Length < 1 || !int.TryParse(args[0], out var pid))
        {
            Console.WriteLine("Usage: HotMethodsCumulative <pid> [windowSec=2] [topNLeaves=50] [filters=foo,bar]");
            return 1;
        }
        int windowSec = args.Length >= 2 && int.TryParse(args[1], out var w) ? Math.Max(1, w) : 2;
        int topNLeaves = args.Length >= 3 && int.TryParse(args[2], out var t) ? Math.Max(1, t) : 50;
        if (args.Length >= 4 && !string.IsNullOrWhiteSpace(args[3]))
            Filters = args[3].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        Task.Run(() => SamplingLoop(pid, windowSec, topNLeaves, cts.Token), cts.Token);
        UiLoop(pid, windowSec, topNLeaves, cts.Token);
        return 0;
    }

    private static void SamplingLoop(int pid, int windowSec, int topNLeaves, CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            string? trace = null, etlx = null;
            try
            {
                trace = CollectWindow(pid, windowSec, requestRundown: true);
                var window = AggregateWindow(trace, out etlx);
                if (window.AvgSampleIntervalMs > 0) LastAvgSampleIntervalMs = window.AvgSampleIntervalMs;
                var intervalMs = LastAvgSampleIntervalMs > 0 ? LastAvgSampleIntervalMs : 1.0;

                IEnumerable<CallGraphNode> nodes = window.Nodes.Values;
                if (Filters.Length > 0)
                    nodes = nodes.Where(n => MatchesNameOnly(n.Name));
                IEnumerable<CallGraphEdge> edges = window.Edges.Values;
                if (Filters.Length > 0)
                    edges = edges.Where(e => MatchesNameOnly(e.Caller) || MatchesNameOnly(e.Callee));

                lock (_lock)
                {
                    foreach (var n in nodes)
                        Graph.AddNodeSamples(n.Name, n.InclusiveSamplesCount, intervalMs);
                    foreach (var e in edges)
                        Graph.AddEdgeSamples(e.Caller, e.Callee, e.SamplesCount, intervalMs);
                    _rows = BuildRows(topNLeaves);
                    _dirty = true;
                }
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[red]sampling error:[/] {Escape(ex.Message)}");
            }
            finally
            {
                TryDelete(trace);
                TryDelete(etlx);
            }
        }
    }

    private static void UiLoop(int pid, int windowSec, int topNLeaves, CancellationToken token)
    {
        int selected = 0;
        int scroll = 0;
        List<DisplayRow> snapshot = new();
        while (!token.IsCancellationRequested)
        {
            if (_dirty)
            {
                lock (_lock)
                {
                    snapshot = _rows;
                    _dirty = false;
                }
                if (selected >= snapshot.Count) selected = Math.Max(0, snapshot.Count - 1);
                EnsureScroll(ref scroll, selected, snapshot.Count);
                Render(pid, windowSec, topNLeaves, snapshot, selected, scroll);
            }

            if (Console.KeyAvailable)
            {
                var k = Console.ReadKey(true);
                switch (k.Key)
                {
                    case ConsoleKey.UpArrow:
                        if (selected > 0) { selected--; EnsureScroll(ref scroll, selected, snapshot.Count); _dirty = true; }
                        break;
                    case ConsoleKey.DownArrow:
                        if (selected < snapshot.Count - 1) { selected++; EnsureScroll(ref scroll, selected, snapshot.Count); _dirty = true; }
                        break;
                    case ConsoleKey.PageUp:
                        { int page = GetUsableRows(); selected = Math.Max(0, selected - page); scroll = Math.Max(0, scroll - page); _dirty = true; }
                        break;
                    case ConsoleKey.PageDown:
                        { int page = GetUsableRows(); selected = Math.Min(snapshot.Count - 1, selected + page); scroll = Math.Min(Math.Max(0, snapshot.Count - page), scroll + page); _dirty = true; }
                        break;
                    case ConsoleKey.Home:
                        selected = 0; scroll = 0; _dirty = true; break;
                    case ConsoleKey.End:
                        { int page = GetUsableRows(); selected = Math.Max(0, snapshot.Count - 1); scroll = Math.Max(0, snapshot.Count - page); _dirty = true; }
                        break;
                    case ConsoleKey.C:
                        lock (_lock)
                        {
                            Graph.Clear();
                            LastAvgSampleIntervalMs = 0;
                            _rows = new();
                            selected = scroll = 0;
                            _dirty = true;
                        }
                        break;
                    case ConsoleKey.T:
                        lock (_lock)
                        {
                            _viewMode = _viewMode == ViewMode.BottomUp ? ViewMode.TopDown : ViewMode.BottomUp;
                            _rows = BuildRows(topNLeaves);
                            selected = scroll = 0;
                            _dirty = true;
                        }
                        break;
                }
            }
            Thread.Sleep(30);
        }
    }

    private static void EnsureScroll(ref int scroll, int selected, int total)
    {
        int usable = GetUsableRows();
        if (selected < scroll) scroll = selected;
        if (selected >= scroll + usable) scroll = Math.Max(0, selected - usable + 1);
        scroll = Math.Min(scroll, Math.Max(0, Math.Max(0, total - usable)));
    }

    private static int GetUsableRows()
    {
        int h; try { h = Console.WindowHeight; } catch { h = 40; }
        int baseOverhead = 1 + 1 + 1 + 2 + 1 + 1; // =7
        int overhead = baseOverhead;
        int usable = h - overhead;
        if (usable < 5) usable = 5;
        return usable;
    }

    private static void Render(int pid, int windowSec, int topNLeaves, List<DisplayRow> rows, int selected, int scroll)
    {
        AnsiConsole.Cursor.Hide();
        AnsiConsole.Clear();
        var filterEsc = Escape(string.Join(", ", Filters));
    var modeLabel = _viewMode == ViewMode.BottomUp ? "BottomUp" : "TopDown";
    var header = new Rule($"PID={pid} | Window={windowSec}s | TopN={topNLeaves} | Filters={filterEsc} | View={modeLabel}") { Justification = Justify.Left };
        AnsiConsole.Write(header);
        AnsiConsole.MarkupLine($"[dim]Updated @ {DateTime.Now:HH:mm:ss} | interval≈{LastAvgSampleIntervalMs:F3} ms | Rows={rows.Count}[/]");
    AnsiConsole.MarkupLine("[grey]Use ↑↓ PgUp PgDn Home End, 'C' clear, 'T' toggle view, Ctrl+C exit[/]");
    int usable = GetUsableRows();
        var table = new Table().Border(TableBorder.SimpleHeavy);
        table.AddColumn("Samples");
        table.AddColumn("CPU-ms");
        table.AddColumn("Percent");
        table.AddColumn("Call Tree (hot leaves)");
        int methodWidth = GetMethodColumnWidth();
        foreach (var row in rows.Skip(scroll).Take(usable))
        {
            int idx = rows.IndexOf(row); // acceptable for limited visible range
            var styleStart = idx == selected ? "[black on yellow]" : string.Empty;
            var styleEnd = idx == selected ? "[/]" : string.Empty;
            var indent = new string(' ', row.Depth * 2);
            var bullet = row.Depth > 0 ? "• " : string.Empty;
            var plainLabel = indent + bullet + row.Name;
            if (plainLabel.Length > methodWidth)
                plainLabel = plainLabel.Substring(0, Math.Max(0, methodWidth - 1)) + "…";
            var label = Escape(plainLabel);
            table.AddRow(
                styleStart + row.Samples.ToString() + styleEnd,
                styleStart + row.CpuMs.ToString("F1") + styleEnd,
                styleStart + row.Percent.ToString("F1") + "%" + styleEnd,
                styleStart + label + styleEnd
            );
        }
        AnsiConsole.Write(table);
        AnsiConsole.MarkupLine($"[dim]Showing {Math.Min(rows.Count, rows.Count == 0 ? 0 : scroll + 1)}-{Math.Min(scroll + usable, rows.Count)} of {rows.Count}[/]");
        AnsiConsole.Cursor.Show();
    }

    private static List<DisplayRow> BuildRows(int topN)
    {
        return _viewMode == ViewMode.BottomUp ? BuildFlattenedBottomUp(topN) : BuildFlattenedTopDown(topN);
    }

    private static List<DisplayRow> BuildFlattenedBottomUp(int topNLeaves)
    {
        var rows = new List<DisplayRow>();
        var outgoing = new Dictionary<string, List<(CallGraphEdge edge, CallGraphNode callee)>>();
        var incoming = new Dictionary<string, List<(CallGraphEdge edge, CallGraphNode caller)>>();
        foreach (var e in Graph.Edges.Values)
        {
            if (!Graph.Nodes.TryGetValue(e.Caller, out var caller) || !Graph.Nodes.TryGetValue(e.Callee, out var callee)) continue;
            if (!outgoing.TryGetValue(e.Caller, out var outList)) outgoing[e.Caller] = outList = new();
            outList.Add((e, callee));
            if (!incoming.TryGetValue(e.Callee, out var inList)) incoming[e.Callee] = inList = new();
            inList.Add((e, caller));
        }
        var leaves = Graph.Nodes.Values.Where(n => !outgoing.ContainsKey(n.Name))
            .OrderByDescending(n => n.InclusiveSamplesCount)
            .Take(topNLeaves)
            .ToList();
        var printed = new HashSet<string>();
        foreach (var leaf in leaves)
        {
            var path = new List<CallGraphNode>();
            var current = leaf; int guard = 0;
            while (current != null && guard++ < 256)
            {
                path.Add(current);
                if (!incoming.TryGetValue(current.Name, out var parents) || parents.Count == 0) break;
                var chosen = parents
                    .OrderByDescending(p => p.caller.InclusiveSamplesCount)
                    .ThenByDescending(p => p.edge.SamplesCount)
                    .First().caller;
                if (path.Any(p => p.Name == chosen.Name)) break;
                current = chosen;
            }
            path.Reverse();
            for (int depth = 0; depth < path.Count; depth++)
            {
                var node = path[depth];
                if (printed.Contains(node.Name)) continue;
                printed.Add(node.Name);
                double pct = (Graph.TotalCpuMsSum > 0) ? (100.0 * node.CpuMs / Graph.TotalCpuMsSum) : 0.0;
                rows.Add(new DisplayRow(node.Name, node.InclusiveSamplesCount, node.CpuMs, pct, depth));
            }
        }
        return rows;
    }

    private static List<DisplayRow> BuildFlattenedTopDown(int topN)
    {
        var rows = new List<DisplayRow>();
        var outgoing = new Dictionary<string, List<(CallGraphEdge edge, CallGraphNode callee)>>();
        var incoming = new HashSet<string>();
        foreach (var e in Graph.Edges.Values)
        {
            if (!Graph.Nodes.TryGetValue(e.Caller, out var caller) || !Graph.Nodes.TryGetValue(e.Callee, out var callee)) continue;
            if (!outgoing.TryGetValue(e.Caller, out var list)) outgoing[e.Caller] = list = new();
            list.Add((e, callee));
            incoming.Add(e.Callee);
        }

        var roots = Graph.Nodes.Values.Where(n => !incoming.Contains(n.Name))
            .OrderByDescending(n => n.InclusiveSamplesCount)
            .Take(topN)
            .ToList();
        if (roots.Count == 0)
        {
            roots = Graph.Nodes.Values.OrderByDescending(n => n.InclusiveSamplesCount).Take(topN).ToList();
        }
        var printed = new HashSet<string>();
        foreach (var root in roots)
        {
            Traverse(root, 0, printed, rows, outgoing, depthLimit: 128);
        }
        return rows;
    }

    private static void Traverse(CallGraphNode node, int depth, HashSet<string> printed, List<DisplayRow> rows,
        Dictionary<string, List<(CallGraphEdge edge, CallGraphNode callee)>> outgoing, int depthLimit)
    {
        if (depth > depthLimit) return;
        if (printed.Contains(node.Name)) return; // avoid repetition & cycles
        printed.Add(node.Name);
        double pct = (Graph.TotalCpuMsSum > 0) ? (100.0 * node.CpuMs / Graph.TotalCpuMsSum) : 0.0;
        rows.Add(new DisplayRow(node.Name, node.InclusiveSamplesCount, node.CpuMs, pct, depth));
        if (!outgoing.TryGetValue(node.Name, out var children)) return;
        foreach (var child in children
            .OrderByDescending(c => c.callee.InclusiveSamplesCount)
            .ThenByDescending(c => c.edge.SamplesCount))
        {
            Traverse(child.callee, depth + 1, printed, rows, outgoing, depthLimit);
        }
    }

    private static string Escape(string? v) => string.IsNullOrEmpty(v) ? string.Empty : v.Replace("[", "[[").Replace("]", "]]");

    private static int GetMethodColumnWidth()
    {
        int w; try { w = Console.WindowWidth; } catch { w = 120; }
        // Reserve widths for fixed numeric columns and borders/padding.
        // Numeric columns target widths: Samples(8), CPU-ms(8), Percent(7)
        // Add separators/padding fudge factor (~14)
        int reserved = 8 + 8 + 7 + 14;
        int method = w - reserved;
        if (method < 20) method = 20;
        return method;
    }

    private static int InitTableMargin()
    {
        var env = Environment.GetEnvironmentVariable("DOTSTACK_TABLE_MARGIN");
        if (int.TryParse(env, out var m) && m >= 0 && m <= 50) return m;
        return 3;
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
}

internal sealed record DisplayRow(string Name, long Samples, double CpuMs, double Percent, int Depth);

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

internal enum ViewMode
{
    BottomUp,
    TopDown
}
