const std = @import("std");
const Pool = @This();

sync: std.atomic.Value(Sync) align(std.atomic.cache_line),
idle: std.atomic.Value(u32) = .{ .raw = 0 },
injector: [2]std.atomic.Value(?*Task) align(std.atomic.cache_line) = .{ .{ .raw = null }, .{ .raw = null } },
workers: std.atomic.Value(?*Worker) align(std.atomic.cache_line) = .{ .raw = null },
join: std.atomic.Value(u32) = .{ .raw = 0 },

const Sync = packed struct(u32) {
    shutdown: bool = false,
    notified: bool = false,
    max: u10 = 0,
    idle: u10 = 0,
    spawnable: u10 = 0,

    fn add(self: Sync, other: Sync) Sync {
        return @bitCast(@as(u32, @bitCast(self)) +% @as(u32, @bitCast(other)));
    }

    fn delta(insert: Sync, remove: Sync) Sync {
        return insert.add(@bitCast(@as(u32, 0) -% @as(u32, @bitCast(remove))));
    }
};

pub fn init(max_threads: usize) Pool {
    const max = std.math.lossyCast(u10, max_threads);
    return .{ .sync = .{ .raw = .{ .max = max, .spawnable = max } } };
}

pub fn deinit(self: *Pool) void {
    const sync = self.sync.fetchAdd(.{ .shutdown = true }, .acq_rel);
    if (sync.idle > 0) {
        _ = self.idle.fetchOr(2, .release);
        std.Thread.Futex.wake(&self.idle, std.math.maxInt(u32));
    }
    if (sync.spawnable == sync.max) self.join.store(2, .monotonic);
    while (self.join.swap(1, .acquire) <= 1) std.Thread.Futex.wait(&self.join, 1);
    self.* = undefined;
}

pub const Task = struct {
    next: std.atomic.Value(?*Task) = .{ .raw = null },
    callback: *const fn (*Task) void,
};

pub fn schedule(noalias self: *Pool, noalias task: *Task) void {
    if (Worker.current) |worker| blk: {
        @branchHint(.likely);
        const tail = worker.tail.raw;
        const head = worker.head.load(.monotonic);
        if (tail -% head == worker.array.len) {
            @branchHint(.unlikely);
            const move = worker.array.len / 2;
            _ = worker.head.cmpxchgStrong(head, head +% move, .acquire, .monotonic) orelse {
                var last = task;
                for (0..move) |i| {
                    last.next.raw = worker.array[(head +% i) % worker.array.len].raw.?;
                    last = last.next.raw.?;
                }
                break :blk inject(&worker.overflow, task, last);
            };
        }
        worker.array[tail % worker.array.len].store(task, .monotonic);
        worker.tail.store(tail +% 1, .release);
    } else inject(&self.injector, task, task);
    self.notify(self.sync.load(.seq_cst));
}

fn notify(self: *Pool, curr: Sync) void {
    if ((curr.shutdown or curr.notified) or (curr.idle | curr.spawnable) == 0) return;
    if (self.sync.fetchOr(.{ .notified = true }, .acquire).notified) return;

    var sync = self.sync.load(.monotonic);
    if (sync.shutdown) return;
    if (sync.idle == 0 and sync.spawnable > 0) blk: {
        sync = self.sync.fetchAdd(Sync.delta(.{ .idle = 1 }, .{ .spawnable = 1 }), .acquire);
        if (sync.shutdown) return self.finish();
        const thread = std.Thread.spawn(.{}, Worker.run, .{self}) catch break :blk self.finish();
        return thread.detach();
    }

    _ = self.idle.fetchOr(1, .release);
    std.Thread.Futex.wake(&self.idle, 1);
}

noinline fn finish(self: *Pool) void {
    const sync = self.sync.fetchAdd(Sync.delta(.{ .spawnable = 1 }, .{ .idle = 1 }), .acq_rel);
    if (sync.shutdown and sync.spawnable + 1 == sync.max) {
        if (self.workers.load(.acquire)) |worker| {
            if (worker.join.swap(2, .release) > 0) std.Thread.Futex.wake(&worker.join, 1);
        }
        if (self.join.swap(2, .release) > 0) std.Thread.Futex.wake(&self.join, 1);
    }
}

fn inject(noalias injector: *[2]std.atomic.Value(?*Task), front: *Task, back: *Task) void {
    back.next.raw = null;
    const link = if (injector[1].swap(back, .acq_rel)) |tail| &tail.next else &injector[0];
    link.store(front, .seq_cst);
}

fn consume(noalias injector: *[2]std.atomic.Value(?*Task), noalias worker: *Worker) ?*align(1) Task {
    if (injector[0].load(.monotonic) == null) return null;
    var p_head: ?*Task = injector[0].swap(null, .acquire) orelse return null;

    const tail = worker.tail.raw;
    const pushed = for (0..worker.array.len) |i| {
        const task = p_head orelse break i;
        worker.array[(tail +% i) % worker.array.len].store(task, .monotonic);
        p_head = task.next.load(.acquire) orelse next: {
            _ = injector[1].cmpxchgStrong(task, null, .seq_cst, .monotonic) orelse break :next null;
            break :next task.next.load(.acquire) orelse break i;
        };
    } else worker.array.len;

    if (p_head) |h| injector[0].store(h, .seq_cst);
    const new_tail = if (pushed > 0) tail +% (pushed - 1) else return null;
    worker.tail.store(new_tail, .release);
    return @ptrFromInt(@intFromPtr(worker.array[new_tail % worker.array.len].raw.?) | @intFromBool(p_head != null));
}

const Worker = struct {
    overflow: [2]std.atomic.Value(?*Task) align(std.atomic.cache_line) = .{ .{ .raw = null }, .{ .raw = null } },
    head: std.atomic.Value(usize) align(std.atomic.cache_line) = .{ .raw = 0 },
    array: [256]std.atomic.Value(?*Task) = undefined,
    tail: std.atomic.Value(usize) = .{ .raw = 0 },
    join: std.atomic.Value(u32) = .{ .raw = 0 },
    active: bool = false,
    target: ?*Worker = null,
    next: ?*Worker,

    threadlocal var current: ?*Worker = null;

    fn run(noalias pool: *Pool) void {
        var self = Worker{ .next = pool.workers.load(.acquire) };
        pool.workers.store(&self, .release);
        current = &self;

        while (self.poll(pool)) |task| task.callback(task);

        pool.finish();
        while (self.join.swap(1, .acquire) <= 1) std.Thread.Futex.wait(&self.join, 1);
        if (self.next) |worker| {
            if (worker.join.swap(2, .release) > 0) std.Thread.Futex.wake(&worker.join, 1);
        }
    }

    fn poll(noalias self: *Worker, noalias pool: *Pool) ?*Task {
        const tail = self.tail.raw;
        const head = self.head.fetchAdd(1, .acquire);
        if (head != tail) return self.array[head % self.array.len].raw.?;
        self.head.store(head, .monotonic);

        const result = consume(&self.overflow, self) orelse steal: while (true) {
            if (consume(&pool.injector, self)) |task| break :steal task;
            const start = self.target;
            while (true) {
                const target = self.target orelse pool.workers.load(.acquire).?;
                self.target = target.next;
                if (consume(&target.overflow, self)) |task| break :steal task;

                var t_head = target.head.load(.acquire);
                while (true) {
                    const t_size = target.tail.load(.acquire) -% t_head;
                    if (@as(isize, @bitCast(t_size)) <= 0) break;

                    const t_steal = t_size - (t_size / 2);
                    for (0..t_steal) |i| {
                        const task = target.array[(t_head +% i) % target.array.len].load(.monotonic);
                        self.array[(tail +% i) % self.array.len].store(task, .monotonic);
                    }

                    t_head = target.head.cmpxchgStrong(t_head, t_head +% t_steal, .acq_rel, .acquire) orelse {
                        const new_tail = tail +% (t_steal - 1);
                        self.tail.store(new_tail, .release);
                        break :steal self.array[new_tail % self.array.len].raw.?;
                    };
                }
                if (self.target == start) break;
            }

            const sync = pool.sync.fetchAdd(Sync.delta(.{ .idle = @intFromBool(self.active) }, .{ .notified = !self.active }), .seq_cst);
            self.active = false;
            if (sync.shutdown) return null;
            if (pool.injector[0].load(.seq_cst) != null and !pool.sync.fetchOr(.{ .notified = true }, .acquire).notified) continue;

            var idle = pool.idle.load(.acquire);
            while (true) {
                while (idle == 0) : (idle = pool.idle.load(.acquire)) std.Thread.Futex.wait(&pool.idle, 0);
                if (idle == 1) idle = pool.idle.cmpxchgStrong(1, 0, .acquire, .acquire) orelse break;
                if (idle >= 2) return null;
            }
        };

        if (!self.active) {
            const stop = Sync.delta(.{}, .{ .notified = true, .idle = 1 });
            pool.notify(pool.sync.fetchAdd(stop, .seq_cst).add(stop));
        } else if (@intFromPtr(result) & 1 > 0) {
            pool.notify(pool.sync.load(.seq_cst));
        }
        self.active = true;
        return @ptrFromInt(@intFromPtr(result) & ~@as(usize, 1));
    }
};
