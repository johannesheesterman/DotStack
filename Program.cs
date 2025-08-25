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
    private static readonly HashSet<string> _collapsed = new(StringComparer.Ordinal);
    private static volatile string? _selectedNode = null; // current focused node for zoom view
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
                            _selectedNode = null;
                            selected = scroll = 0;
                            _dirty = true;
                        }
                        break;
                    case ConsoleKey.Enter:
                        if (snapshot.Count > 0)
                        {
                            var row = snapshot[selected];
                            if (!string.IsNullOrEmpty(row.Name))
                            {
                                lock (_lock)
                                {
                                    _selectedNode = row.Name;
                                    _rows = BuildRows(topNLeaves);
                                    selected = 0; scroll = 0; _dirty = true;
                                }
                            }
                        }
                        break;
                    case ConsoleKey.Escape:
                        if (_selectedNode != null)
                        {
                            lock (_lock)
                            {
                                _selectedNode = null;
                                _rows = BuildRows(topNLeaves);
                                selected = 0; scroll = 0; _dirty = true;
                            }
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
    var topLabel = topNLeaves <= 0 ? "All" : topNLeaves.ToString();
    var header = new Rule($"PID={pid} | Window={windowSec}s | TopN={topLabel} | Filters={filterEsc}") { Justification = Justify.Left };
        AnsiConsole.Write(header);
        AnsiConsole.MarkupLine($"[dim]Updated @ {DateTime.Now:HH:mm:ss} | interval≈{LastAvgSampleIntervalMs:F3} ms | Rows={rows.Count}[/]");
    var modeInfo = _selectedNode == null ? "Global view" : $"Zoom: {_selectedNode}";
    AnsiConsole.MarkupLine($"[grey]{modeInfo} | Enter=select zoom  Esc=exit zoom | ↑↓ PgUp PgDn Home End | ← collapse → expand | C clear | Ctrl+C exit[/]");
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
            var bullet = row.Depth > 0 ? string.Empty : string.Empty;
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
        if (_selectedNode is { Length: >0 } name && Graph.Nodes.ContainsKey(name))
            return BuildZoomUnified(name, topN);
        return BuildFlattenedByNodeSamples(topN);
    }

    private static bool RowHasChildren(List<DisplayRow> rows, int index)
    {
        if (index < 0 || index >= rows.Count) return false;
        int d = rows[index].Depth;
        int next = index + 1;
        return next < rows.Count && rows[next].Depth == d + 1;
    }

    private static List<DisplayRow> BuildFlattenedByNodeSamples(int topN)
    {
        var rows = new List<DisplayRow>();
        var outgoing = new Dictionary<string, List<(CallGraphEdge edge, CallGraphNode callee)>>();
        foreach (var e in Graph.Edges.Values)
        {
            if (!outgoing.TryGetValue(e.Caller.Name, out var list)) outgoing[e.Caller.Name] = list = new();
            list.Add((e, e.Callee));
        }
        IEnumerable<CallGraphNode> rootCandidates = Graph.Nodes.Values
            .OrderByDescending(n => n.InclusiveSamplesCount)
            .ThenBy(n => n.Name, StringComparer.Ordinal);
        if (topN > 0) rootCandidates = rootCandidates.Take(topN);
        var displayed = new HashSet<string>(StringComparer.Ordinal);
        foreach (var root in rootCandidates)
        {
            if (displayed.Contains(root.Name))
                continue;
            TraverseCallees(root, null, 0, new HashSet<string>(), rows, outgoing, 128, displayed);
        }
        return rows;
    }

    // Unified zoom: callers above focus, focus row, then callees below in one tree
    private static List<DisplayRow> BuildZoomUnified(string focusName, int topN)
    {
        var rows = new List<DisplayRow>();
        if (!Graph.Nodes.TryGetValue(focusName, out var focus)) return rows;

        // Build maps
        var parentsByCallee = new Dictionary<string, List<(CallGraphEdge edge, CallGraphNode parent)>>();
        var outgoing = new Dictionary<string, List<(CallGraphEdge edge, CallGraphNode callee)>>();
        foreach (var e in Graph.Edges.Values)
        {
            if (!outgoing.TryGetValue(e.Caller.Name, out var outList)) outgoing[e.Caller.Name] = outList = new();
            outList.Add((e, e.Callee));
            if (!parentsByCallee.TryGetValue(e.Callee.Name, out var inList)) parentsByCallee[e.Callee.Name] = inList = new();
            inList.Add((e, e.Caller));
        }

        // Gather ancestors reachable to focus
        var ancestorSet = new HashSet<string>(StringComparer.Ordinal);
        var stack = new Stack<string>();
        stack.Push(focusName);
        while (stack.Count > 0)
        {
            var cur = stack.Pop();
            if (!parentsByCallee.TryGetValue(cur, out var ps)) continue;
            foreach (var p in ps)
            {
                if (ancestorSet.Add(p.parent.Name)) stack.Push(p.parent.Name);
            }
        }

        // Determine ancestor roots (no parents in ancestorSet)
        var ancestorRoots = ancestorSet.Where(a =>
            !parentsByCallee.TryGetValue(a, out var ps) || ps.All(p => !ancestorSet.Contains(p.parent.Name)))
            .Select(a => Graph.Nodes[a])
            .OrderByDescending(n => n.InclusiveSamplesCount)
            .ThenBy(n => n.Name, StringComparer.Ordinal)
            .ToList();

        // Helper: does a node lead to focus through ancestors only?
        var memoLeads = new Dictionary<string, bool>(StringComparer.Ordinal);
        bool LeadsToFocus(string name)
        {
            if (name == focusName) return true;
            if (memoLeads.TryGetValue(name, out var v)) return v;
            // Provisional value to break cycles: assume false until proven true
            memoLeads[name] = false;
            if (outgoing.TryGetValue(name, out var outs))
            {
                foreach (var c in outs)
                {
                    var childName = c.callee.Name;
                    if (childName == focusName || ancestorSet.Contains(childName))
                    {
                        if (LeadsToFocus(childName)) { memoLeads[name] = true; break; }
                    }
                }
            }
            return memoLeads[name];
        }

        // Build caller side (excluding focus) depths starting at 0
        var callerRows = new List<DisplayRow>();
        int maxCallerDepth = -1;
        void TraverseAncestors(CallGraphNode node, int depth, HashSet<string> path)
        {
            if (depth > 64) return;
            if (node.Name == focusName)
            {
                if (depth - 1 > maxCallerDepth) maxCallerDepth = depth - 1;
                return; // don't add focus in callers section
            }
            bool cycle = path.Contains(node.Name);
            bool hasPathChild = false;
            if (!cycle && outgoing.TryGetValue(node.Name, out var outs))
            {
                foreach (var c in outs)
                {
                    if (c.callee.Name == focusName || ancestorSet.Contains(c.callee.Name))
                    {
                        if (LeadsToFocus(c.callee.Name)) { hasPathChild = true; break; }
                    }
                }
            }
            callerRows.Add(new DisplayRow(cycle ? node.Name + " (cycle)" : node.Name, node.InclusiveSamplesCount, node.CpuMs, Percent(node.CpuMs), depth, hasPathChild));
            if (cycle) return;
            path.Add(node.Name);
            if (outgoing.TryGetValue(node.Name, out var children))
            {
                foreach (var child in children
                    .OrderByDescending(c => c.edge.SamplesCount)
                    .ThenBy(c => c.callee.Name, StringComparer.Ordinal))
                {
                    if (child.callee.Name == focusName || ancestorSet.Contains(child.callee.Name))
                    {
                        if (LeadsToFocus(child.callee.Name))
                            TraverseAncestors(child.callee, depth + 1, path);
                    }
                }
            }
            path.Remove(node.Name);
        }

        foreach (var root in ancestorRoots)
            TraverseAncestors(root, 0, new HashSet<string>());

        // Focus depth is maxCallerDepth + 1
        int focusDepth = maxCallerDepth + 1;
        // Adjust nothing (callers start at 0 already), we just insert focus row after callers section.
        rows.AddRange(callerRows);

        bool focusHasChildren = outgoing.TryGetValue(focusName, out var focusOuts) && focusOuts.Count > 0;
        rows.Add(new DisplayRow(focusName, focus.InclusiveSamplesCount, focus.CpuMs, Percent(focus.CpuMs), focusDepth, focusHasChildren));

        // Callees side
        void TraverseCalleesLocal(CallGraphNode node, int depth, HashSet<string> path)
        {
            if (depth > 128) return;
            bool cycle = path.Contains(node.Name);
            bool hasChildren = !cycle && outgoing.TryGetValue(node.Name, out var list) && list.Count > 0;
            if (node.Name != focusName) // focus already added
                rows.Add(new DisplayRow(cycle ? node.Name + " (cycle)" : node.Name, node.InclusiveSamplesCount, node.CpuMs, Percent(node.CpuMs), depth, hasChildren));
            if (cycle) return;
            path.Add(node.Name);
            if (!_collapsed.Contains(node.Name) && outgoing.TryGetValue(node.Name, out var outs))
            {
                foreach (var c in outs
                    .OrderByDescending(c => c.edge.SamplesCount)
                    .ThenBy(c => c.callee.Name, StringComparer.Ordinal))
                {
                    TraverseCalleesLocal(c.callee, depth + 1, path);
                }
            }
            path.Remove(node.Name);
        }
        if (!_collapsed.Contains(focusName) && focusHasChildren)
        {
            foreach (var c in focusOuts!
                .OrderByDescending(c => c.edge.SamplesCount)
                .ThenBy(c => c.callee.Name, StringComparer.Ordinal))
            {
                TraverseCalleesLocal(c.callee, focusDepth + 1, new HashSet<string> { focusName });
            }
        }
        return rows;
    }

    private static void TraverseCallees(CallGraphNode node, CallGraphEdge? incomingEdge, int depth, HashSet<string> path, List<DisplayRow> rows,
        Dictionary<string, List<(CallGraphEdge edge, CallGraphNode callee)>> outgoing, int depthLimit, HashSet<string> displayed)
    {
        if (depth > depthLimit) return;
        bool cycle = path.Contains(node.Name);
        long samplesDisplay = incomingEdge?.SamplesCount ?? node.InclusiveSamplesCount;
        double cpuMsDisplay = incomingEdge?.CpuMs ?? node.CpuMs;
        bool hasChildren = !cycle && outgoing.TryGetValue(node.Name, out var childList) && childList.Count > 0;
        rows.Add(new DisplayRow(cycle ? node.Name + " (cycle)" : node.Name, samplesDisplay, cpuMsDisplay, Percent(cpuMsDisplay), depth, hasChildren));
        if (cycle) return;
        displayed.Add(node.Name);
        path.Add(node.Name);
        if (!_collapsed.Contains(node.Name) && outgoing.TryGetValue(node.Name, out var children))
        {
            foreach (var child in children
                .OrderByDescending(c => c.edge.SamplesCount)
                .ThenBy(c => c.callee.Name, StringComparer.Ordinal))
            {
                TraverseCallees(child.callee, child.edge, depth + 1, path, rows, outgoing, depthLimit, displayed);
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