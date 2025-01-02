const std = @import("std");

/// Old thread pool from the blog
/// https://zig.news/kprotty/resource-efficient-thread-pools-with-zig-3291
const V1 = struct {
    pub const Pool = @import("v1.zig");
    pub const Task = Pool.Task;

    pub fn init(max_threads: usize) Pool {
        return Pool.init(.{ .max_threads = std.math.lossyCast(u32, max_threads) });
    }

    pub fn deinit(pool: *Pool) void {
        pool.shutdown();
        pool.deinit();
    }

    pub fn schedule(noalias pool: *Pool, noalias task: *Task) void {
        pool.schedule(Pool.Batch.from(task));
    }
};

/// New thread pool optimized for wait-free operations and LOC
/// https://x.com/kingprotty/status/1873793282612060580
const V2 = struct {
    pub const Pool = @import("v2.zig");
    pub const Task = Pool.Task;
    pub const init = Pool.init;
    pub const deinit = Pool.deinit;
    pub const schedule = Pool.schedule;
};

/// Stub impl to query the internal size parameter for each benchmark.
const VSizeQuery = struct {
    pub const Pool = struct {};
};

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var timer = try std.time.Timer.start();
    var writer = std.io.getStdOut().writer();

    // Get num_cpus either from cli arg or through std.Thread.getCpuCount().
    const num_cpus = blk: {
        var args = try std.process.ArgIterator.initWithAllocator(allocator);
        defer args.deinit();

        _ = args.skip();

        const arg = args.next() orelse break :blk @max(1, std.Thread.getCpuCount() catch 1);
        break :blk std.fmt.parseInt(u32, arg, 10) catch @panic("arg must be a u32");
    };

    var v1 = V1.init(num_cpus);
    defer V1.deinit(&v1);

    var v2 = V2.init(num_cpus);
    defer V2.deinit(&v2);

    inline for (.{
        .{ "v1", V1, &v1 },
        .{ "v2", V2, &v2 },
    }) |zap| {
        try writer.print("warmup {s} ..\r", .{zap[0]});
        try warmup(zap[1], zap[2], .{ .allocator = allocator, .n = num_cpus });
    }

    inline for (.{
        .{ "outside pool", benchOuter },
        .{ "inside pool", benchInner },
        .{ "yield", benchYield },
        .{ "rand work", benchRand },
    }) |bench| {
        for (0..2) |i| {
            const n = if (i == 0) 1 else num_cpus;
            if (i == 1 and num_cpus <= 1) continue; // skip if num_cpus=1 to avoid same thing.

            const size = try bench[1](VSizeQuery, undefined, undefined);
            if (size > 0) {
                try writer.print("{} {s} (size = {}):\n", .{ n, bench[0], size });
            } else {
                try writer.print("{} {s}:\n", .{ n, bench[0] });
            }

            inline for (.{
                .{ "v1", V1, &v1 },
                .{ "v2", V2, &v2 },
            }) |zap| {
                var samples: [10]u64 = undefined;
                for (&samples, 0..) |*s, idx| {
                    try writer.print("    {s} {}/{}\r", .{ zap[0], idx, samples.len });
                    _ = arena.reset(.retain_capacity);

                    const start = timer.read();
                    _ = try bench[1](zap[1], zap[2], .{ .allocator = allocator, .n = n });
                    s.* = timer.read() - start;
                }

                const vec: @Vector(samples.len, u64) = samples;
                const min = @reduce(.Min, vec);
                const max = @reduce(.Max, vec);
                const mean = @as(f64, @floatFromInt(@reduce(.Add, vec))) / @as(f64, @floatFromInt(samples.len));

                var stdev: f64 = 0.0;
                for (samples) |s| {
                    const r = @as(f64, @floatFromInt(s)) - mean;
                    stdev += r * r;
                }
                if (samples.len > 1) stdev = @sqrt(stdev / (@as(f64, @floatFromInt(samples.len)) - 1));
                try writer.print("    {s}: {: >9.2} +- {: <9.2} ({: <9.2} .. {:.3})\n", .{
                    zap[0],
                    std.fmt.fmtDuration(@as(u64, @intFromFloat(mean))),
                    std.fmt.fmtDuration(@as(u64, @intFromFloat(stdev))),
                    std.fmt.fmtDuration(min),
                    std.fmt.fmtDuration(max),
                });
            }
        }
    }
}

const BenchContext = struct {
    allocator: std.mem.Allocator,
    n: usize,
};

/// Spawn num_cpus tasks which just sleep for this. This forces Zap to do the lazy std.Thread.spawn's
/// so that they don't occur during the benchmark Zap.schedules.
fn warmup(comptime Zap: type, pool: *Zap.Pool, ctx: BenchContext) !void {
    const Work = struct {
        task: Zap.Task = .{ .callback = @This().callback },
        wg: *std.Thread.WaitGroup,

        fn callback(task: *Zap.Task) void {
            const self: *@This() = @alignCast(@fieldParentPtr("task", task));
            std.time.sleep(100 * std.time.ns_per_ms);
            self.wg.finish();
        }
    };

    const works = try ctx.allocator.alloc(Work, ctx.n);
    defer ctx.allocator.free(works);

    var wg = std.Thread.WaitGroup{};
    defer wg.wait();

    for (works) |*w| {
        wg.start();
        w.* = .{ .wg = &wg };
        Zap.schedule(pool, &w.task);
    }
}

/// Scheduling tasks from outside the pool.
fn benchOuter(comptime Zap: type, pool: *Zap.Pool, ctx: BenchContext) !usize {
    const total = 10_000_000;
    if (Zap == VSizeQuery) return total;

    const Work = struct {
        task: Zap.Task = .{ .callback = @This().callback },
        wg: *std.Thread.WaitGroup,

        fn callback(task: *Zap.Task) void {
            const self: *@This() = @alignCast(@fieldParentPtr("task", task));
            self.wg.finish();
        }
    };

    const Spawner = struct {
        fn run(p: *Zap.Pool, works: []Work, wg: *std.Thread.WaitGroup) void {
            defer wg.finish();
            for (works) |*w| {
                w.* = .{ .wg = wg };
                wg.start();
                Zap.schedule(p, &w.task);
            }
        }
    };

    const works = try ctx.allocator.alloc(Work, total);
    defer ctx.allocator.free(works);

    var wg = std.Thread.WaitGroup{};
    defer wg.wait();

    for (0..ctx.n) |i| {
        wg.start();
        const wc = works[i * (works.len / ctx.n) ..];
        (try std.Thread.spawn(.{}, Spawner.run, .{ pool, wc[0..@min(wc.len, works.len / ctx.n)], &wg })).detach();
    }

    return undefined;
}

/// Scheduling tasks from inside the pool.
fn benchInner(comptime Zap: type, pool: *Zap.Pool, ctx: BenchContext) !usize {
    const total = 10_000_000;
    if (Zap == VSizeQuery) return total;

    const Work = struct {
        task: Zap.Task = .{ .callback = @This().callback },
        wg: *std.Thread.WaitGroup,

        fn callback(task: *Zap.Task) void {
            const self: *@This() = @alignCast(@fieldParentPtr("task", task));
            self.wg.finish();
        }
    };

    const Spawner = struct {
        task: Zap.Task = .{ .callback = @This().callback },
        wg: *std.Thread.WaitGroup,
        works: []Work,
        p: *Zap.Pool,

        fn callback(task: *Zap.Task) void {
            const self: *@This() = @alignCast(@fieldParentPtr("task", task));
            defer self.wg.finish();
            for (self.works) |*w| {
                w.* = .{ .wg = self.wg };
                self.wg.start();
                Zap.schedule(self.p, &w.task);
            }
        }
    };

    const works = try ctx.allocator.alloc(Work, total);
    defer ctx.allocator.free(works);

    const spawners = try ctx.allocator.alloc(Spawner, ctx.n);
    defer ctx.allocator.free(spawners);

    var wg = std.Thread.WaitGroup{};
    defer wg.wait();

    for (spawners, 0..) |*s, i| {
        wg.start();
        const wc = works[i * (works.len / ctx.n) ..];
        s.* = .{ .p = pool, .wg = &wg, .works = wc[0..@min(wc.len, works.len / ctx.n)] };
        Zap.schedule(pool, &s.task);
    }

    return undefined;
}

/// Spawn tasks which continuously reschedule themselves.
fn benchYield(comptime Zap: type, pool: *Zap.Pool, ctx: BenchContext) !usize {
    const total = 10_000_000;
    if (Zap == VSizeQuery) return total;

    const Work = struct {
        task: Zap.Task = .{ .callback = @This().callback },
        wg: *std.Thread.WaitGroup,
        count: usize,
        p: *Zap.Pool,

        fn callback(task: *Zap.Task) void {
            const self: *@This() = @alignCast(@fieldParentPtr("task", task));
            if (self.count == 0) return self.wg.finish();
            self.count -= 1;
            self.task = .{ .callback = @This().callback };
            Zap.schedule(self.p, &self.task);
        }
    };

    const works = try ctx.allocator.alloc(Work, ctx.n);
    defer ctx.allocator.free(works);

    var wg = std.Thread.WaitGroup{};
    defer wg.wait();

    var count: usize = total;
    for (works) |*w| {
        wg.start();
        w.* = .{ .p = pool, .wg = &wg, .count = @min(count, total / ctx.n) };
        count -= w.count;
        Zap.schedule(pool, &w.task);
    }

    return undefined;
}

/// Spawn tasks which block for a random amount of time.
fn benchRand(comptime Zap: type, pool: *Zap.Pool, ctx: BenchContext) !usize {
    if (Zap == VSizeQuery) return 0;

    const Work = struct {
        task: Zap.Task = .{ .callback = @This().callback },
        wg: *std.Thread.WaitGroup,
        prng: std.Random.DefaultPrng,
        count: usize,
        p: *Zap.Pool,

        fn callback(task: *Zap.Task) void {
            const self: *@This() = @alignCast(@fieldParentPtr("task", task));
            if (self.count == 0) return self.wg.finish();
            self.count -= 1;

            const spin = self.prng.random().uintAtMost(u32, 100_000);
            for (0..spin) |_| std.atomic.spinLoopHint();

            self.task = .{ .callback = @This().callback };
            Zap.schedule(self.p, &self.task);
        }
    };

    const works = try ctx.allocator.alloc(Work, ctx.n);
    defer ctx.allocator.free(works);

    var wg = std.Thread.WaitGroup{};
    defer wg.wait();

    var prng = std.Random.DefaultPrng.init(42);
    for (works) |*w| {
        wg.start();
        w.* = .{
            .p = pool,
            .wg = &wg,
            .prng = std.Random.DefaultPrng.init(prng.random().int(u64)),
            .count = prng.random().uintAtMost(usize, 1000),
        };
        Zap.schedule(pool, &w.task);
    }

    return undefined;
}
