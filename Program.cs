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
    private static readonly HashSet<string> _collapsed = new(StringComparer.Ordinal);
    private const int ConsoleOverheadRows = 7; // header + help + footer spacing
    private static double Percent(double cpuMs) => Graph.TotalCpuMsSum > 0 ? 100.0 * cpuMs / Graph.TotalCpuMsSum : 0.0;
    private static bool IsFilteredName(string name) => Filters.Length == 0 || Filters.Any(f => !string.IsNullOrEmpty(f) && name.IndexOf(f, StringComparison.OrdinalIgnoreCase) >= 0);

    static int Main(string[] args)
    {
        if (args.Length < 1 || !int.TryParse(args[0], out var pid))
        {
            Console.WriteLine("Usage: HotMethodsCumulative <pid> [windowSec=2] [topN=50|0=all] [filters=foo,bar]");
            return 1;
        }
        int windowSec = args.Length >= 2 && int.TryParse(args[1], out var w) ? Math.Max(1, w) : 2;
        int topNLeaves = args.Length >= 3 && int.TryParse(args[2], out var t) ? t : 50; // t <=0 means unlimited
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
                    nodes = nodes.Where(n => IsFilteredName(n.Name));
                IEnumerable<CallGraphEdge> edges = window.Edges.Values;
                if (Filters.Length > 0)
                    edges = edges.Where(e => IsFilteredName(e.Caller.Name) || IsFilteredName(e.Callee.Name));

                lock (_lock)
                {
                    foreach (var n in nodes)
                        Graph.AddNodeSamples(n.Name, n.InclusiveSamplesCount, intervalMs);
                    foreach (var e in edges)
                        Graph.AddEdgeSamples(e.Caller.Name, e.Callee.Name, e.SamplesCount, intervalMs);
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
                    case ConsoleKey.LeftArrow:
                        if (snapshot.Count > 0)
                        {
                            var row = snapshot[selected];
                            lock (_lock)
                            {
                                if (!_collapsed.Contains(row.Name))
                                {
                                    _collapsed.Add(row.Name);
                                    _rows = BuildRows(topNLeaves);
                                    snapshot = _rows;
                                }
                                else
                                {
                                    int curDepth = row.Depth;
                                    for (int i = selected - 1; i >= 0; i--)
                                    {
                                        if (snapshot[i].Depth < curDepth)
                                        {
                                            selected = i;
                                            break;
                                        }
                                    }
                                }
                                _dirty = true;
                            }
                        }
                        break;
                    case ConsoleKey.RightArrow:
                        if (snapshot.Count > 0)
                        {
                            var row = snapshot[selected];
                            lock (_lock)
                            {
                                if (_collapsed.Remove(row.Name))
                                {
                                    _rows = BuildRows(topNLeaves);
                                    snapshot = _rows;
                                    _dirty = true;
                                }
                                else
                                {
                                    if (selected + 1 < snapshot.Count && snapshot[selected + 1].Depth == row.Depth + 1)
                                    {
                                        selected = selected + 1;
                                        _dirty = true;
                                    }
                                }
                            }
                        }
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
                            _collapsed.Clear();
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
        scroll = Math.Clamp(scroll, 0, Math.Max(0, total - usable));
        if (selected < scroll) scroll = selected;
        else if (selected >= scroll + usable) scroll = Math.Max(0, selected - usable + 1);
    }

    private static int GetUsableRows()
    {
        int h; try { h = Console.WindowHeight; } catch { h = 40; }
        int usable = h - ConsoleOverheadRows;
        if (usable < 5) usable = 5;
        return usable;
    }

    private static void Render(int pid, int windowSec, int topNLeaves, List<DisplayRow> rows, int selected, int scroll)
    {
        AnsiConsole.Cursor.Hide();
        AnsiConsole.Clear();
        var filterEsc = Escape(string.Join(", ", Filters));
        var modeLabel = _viewMode == ViewMode.BottomUp ? "BottomUp" : "TopDown";
        var topLabel = topNLeaves <= 0 ? "All" : topNLeaves.ToString();
        var header = new Rule($"PID={pid} | Window={windowSec}s | TopN={topLabel} | Filters={filterEsc} | View={modeLabel}") { Justification = Justify.Left };
        AnsiConsole.Write(header);
        AnsiConsole.MarkupLine($"[dim]Updated @ {DateTime.Now:HH:mm:ss} | interval≈{LastAvgSampleIntervalMs:F3} ms | Rows={rows.Count}[/]");
    AnsiConsole.MarkupLine("[grey]Use ↑↓ PgUp PgDn Home End, ← collapse, → expand, 'C' clear, 'T' toggle view, Ctrl+C exit[/]");
        int usable = GetUsableRows();
        var table = new Table().Border(TableBorder.SimpleHeavy)
            .AddColumn("Samples")
            .AddColumn("CPU-ms")
            .AddColumn("Percent")
            .AddColumn("Call Tree (hot leaves)");
        int methodWidth = GetMethodColumnWidth();
        var visible = rows.Skip(scroll).Take(usable).ToList();
        for (int i = 0; i < visible.Count; i++)
        {
            var row = visible[i];
            int idx = scroll + i;
            bool isSel = idx == selected;
            string Wrap(string s) => isSel ? "[black on yellow]" + s + "[/]" : s;
            if (string.IsNullOrEmpty(row.Name))
            {
                var sep = new string('─', Math.Clamp(methodWidth, 3, 60));
                table.AddRow("", "", "", Wrap("[dim]" + sep + "[/]"));
                continue;
            }
            var indent = new string(' ', row.Depth * 2);
            string marker = string.Empty;
            if (row.HasChildren)
            {
                marker = _collapsed.Contains(row.Name) ? "+ " : "- ";
            }
            var bullet = row.Depth > 0 ? string.Empty : string.Empty; // marker replaces bullet
            var plainLabel = indent + marker + bullet + row.Name;
            if (plainLabel.Length > methodWidth)
                plainLabel = plainLabel[..Math.Max(0, methodWidth - 1)] + "…";
            var label = Escape(plainLabel);
            table.AddRow(
                Wrap(row.Samples.ToString()),
                Wrap(row.CpuMs.ToString("F1")),
                Wrap(row.Percent.ToString("F1") + "%"),
                Wrap(label)
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

    private static bool RowHasChildren(List<DisplayRow> rows, int index)
    {
        if (index < 0 || index >= rows.Count) return false;
        int d = rows[index].Depth;
        int next = index + 1;
        return next < rows.Count && rows[next].Depth == d + 1;
    }

    private static List<DisplayRow> BuildFlattenedBottomUp(int topNLeaves)
    {
        var rows = new List<DisplayRow>();
        var parentsByCallee = new Dictionary<string, HashSet<CallGraphNode>>();
        var hasOutgoing = new HashSet<string>();
        foreach (var e in Graph.Edges.Values)
        {
            hasOutgoing.Add(e.Caller.Name);
            if (!parentsByCallee.TryGetValue(e.Callee.Name, out var set)) parentsByCallee[e.Callee.Name] = set = new();
            set.Add(e.Caller);
        }
        var leavesSeq = Graph.Nodes.Values.Where(n => !hasOutgoing.Contains(n.Name))
            .OrderByDescending(n => n.InclusiveSamplesCount)
            .ThenBy(n => n.Name, StringComparer.Ordinal)
            .AsEnumerable();
        if (topNLeaves > 0) leavesSeq = leavesSeq.Take(topNLeaves);
        var leaves = leavesSeq.ToList();

    void Recurse(string calleeName, HashSet<string> path, int depth)
        {
            if (depth > 128) return;
            if (!parentsByCallee.TryGetValue(calleeName, out var parents)) return;
            var orderedParents = parents
                .Select(p => (Parent: p, Edge: Graph.Edges.TryGetValue((p.Name, calleeName), out var edge) ? edge : null))
                .Where(t => t.Edge != null)
                .OrderByDescending(t => t.Edge!.SamplesCount)
                .ThenBy(t => t.Parent.Name, StringComparer.Ordinal);
            foreach (var tuple in orderedParents)
            {
                var parent = tuple.Parent;
                var edge = tuple.Edge!; // non-null ensured above
                bool cycle = path.Contains(parent.Name);
                long edgeSamples = edge.SamplesCount;
                double edgeCpuMs = edge.CpuMs;
        bool parentHasParents = parentsByCallee.ContainsKey(parent.Name) && !cycle;
        rows.Add(new DisplayRow(cycle ? parent.Name + " (cycle)" : parent.Name, edgeSamples, edgeCpuMs, Percent(edgeCpuMs), depth, parentHasParents));
                if (cycle) continue;
        if (_collapsed.Contains(parent.Name)) continue;
                path.Add(parent.Name);
                Recurse(parent.Name, path, depth + 1);
                path.Remove(parent.Name);
            }
        }

        foreach (var leaf in leaves)
        {
            bool hasParents = parentsByCallee.ContainsKey(leaf.Name);
            rows.Add(new DisplayRow(leaf.Name, leaf.InclusiveSamplesCount, leaf.CpuMs, Percent(leaf.CpuMs), 0, hasParents));
            if (!_collapsed.Contains(leaf.Name))
                Recurse(leaf.Name, new HashSet<string> { leaf.Name }, 1);
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
            var caller = e.Caller;
            var callee = e.Callee;
            if (!outgoing.TryGetValue(caller.Name, out var list)) outgoing[caller.Name] = list = new();
            list.Add((e, callee));
            incoming.Add(callee.Name);
        }

        long OutgoingSamples(CallGraphNode n) => outgoing.TryGetValue(n.Name, out var list) ? list.Sum(t => t.edge.SamplesCount) : 0;
        IEnumerable<CallGraphNode> rootQuery = Graph.Nodes.Values.Where(n => !incoming.Contains(n.Name))
            .OrderByDescending(n => OutgoingSamples(n))
            .ThenByDescending(n => n.InclusiveSamplesCount)
            .ThenBy(n => n.Name, StringComparer.Ordinal);
        if (topN > 0)
            rootQuery = rootQuery.Take(topN);
        var roots = rootQuery.ToList();
        if (roots.Count == 0)
        {
            IEnumerable<CallGraphNode> fallback = Graph.Nodes.Values
                .OrderByDescending(n => OutgoingSamples(n))
                .ThenByDescending(n => n.InclusiveSamplesCount)
                .ThenBy(n => n.Name, StringComparer.Ordinal);
            if (topN > 0) fallback = fallback.Take(topN);
            roots = fallback.ToList();
        }
        foreach (var root in roots)
        {
            TraverseFull(root, null, 0, new HashSet<string>(), rows, outgoing, depthLimit: 128);
        }
        return rows;
    }

    private static void TraverseFull(CallGraphNode node, CallGraphEdge? incomingEdge, int depth, HashSet<string> path, List<DisplayRow> rows,
        Dictionary<string, List<(CallGraphEdge edge, CallGraphNode callee)>> outgoing, int depthLimit)
    {
        if (depth > depthLimit) return;
        bool cycle = path.Contains(node.Name);
        long samplesDisplay = incomingEdge?.SamplesCount ?? node.InclusiveSamplesCount;
        double cpuMsDisplay = incomingEdge?.CpuMs ?? node.CpuMs;
        bool hasChildren = !cycle && outgoing.TryGetValue(node.Name, out var childList) && childList.Count > 0;
        rows.Add(new DisplayRow(cycle ? node.Name + " (cycle)" : node.Name, samplesDisplay, cpuMsDisplay, Percent(cpuMsDisplay), depth, hasChildren));
        if (cycle) return;
        path.Add(node.Name);
        if (!_collapsed.Contains(node.Name) && outgoing.TryGetValue(node.Name, out var children))
        {
            foreach (var child in children
                .OrderByDescending(c => c.callee.InclusiveSamplesCount)
                .ThenByDescending(c => c.edge.SamplesCount)
                .ThenBy(c => c.callee.Name, StringComparer.Ordinal))
            {
                TraverseFull(child.callee, child.edge, depth + 1, path, rows, outgoing, depthLimit);
            }
        }
        path.Remove(node.Name);
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
                chosen = FrameNameUtil.NormalizeFrameName(chosen);
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
                        {
                            edgeObj.SamplesCount++;
                        }
                        else
                        {
                            var callerNode = counts[chosen];
                            var calleeNode = counts[calleeBelow];
                            edgeCounts[edgeKey] = new CallGraphEdge(callerNode, calleeNode, 1);
                        }
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


    static void TryDelete(string? path)
    {
        try { if (!string.IsNullOrEmpty(path) && File.Exists(path)) File.Delete(path); }
        catch { /* ignore */ }
    }
}

internal static class FrameNameUtil
{
    public static string NormalizeFrameName(string? name)
    {
        if (string.IsNullOrEmpty(name)) return string.Empty;
        var sb = new System.Text.StringBuilder(name.Length);
        bool lastSpace = false;
        foreach (var ch in name)
        {
            if (char.IsControl(ch) || char.IsWhiteSpace(ch))
            {
                if (!lastSpace)
                {
                    sb.Append(' ');
                    lastSpace = true;
                }
            }
            else
            {
                sb.Append(ch);
                lastSpace = false;
            }
        }
        return sb.ToString().Trim();
    }
}


internal sealed record DisplayRow(string Name, long Samples, double CpuMs, double Percent, int Depth, bool HasChildren = false);

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
    public CallGraphNode Caller { get; }
    public CallGraphNode Callee { get; }
    public long SamplesCount { get; set; }
    public double CpuMs { get; set; }
    public CallGraphEdge(CallGraphNode caller, CallGraphNode callee, long samples)
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

    public CallGraphNode AddNodeSamples(string name, long samples, double intervalMs)
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
        return node;
    }

    public void AddEdgeSamples(string callerName, string calleeName, long samples, double intervalMs)
    {
        // Ensure node objects exist (and are reused) before creating edge.
        var caller = AddNodeSamples(callerName, 0, 0); // 0 additional samples here
        var callee = AddNodeSamples(calleeName, 0, 0);
        var key = (Caller: callerName, Callee: calleeName);
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
