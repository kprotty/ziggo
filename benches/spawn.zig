const std = @import("std");
const zap = @import("zap");

const Task = zap.runtime.Task;
const Heap = @import("./allocator.zig").Allocator;

const num_spawners = 10;
const num_tasks = 100_000;

pub fn main() !void {
    var heap: Heap = undefined;
    try heap.init();
    defer heap.deinit();

    try (try Task.runAsync(.{}, asyncMain, .{heap.getAllocator()}));
}

fn asyncMain(allocator: *std.mem.Allocator) !void {
    const frames = try allocator.alloc(@Frame(spawner), num_spawners);
    defer allocator.free(frames);

    var counter: usize = num_tasks * num_spawners;
    for (frames) |*frame|
        frame.* = async spawner(allocator, &counter);
    for (frames) |*frame|
        try (await frame);

    const count = @atomicLoad(usize, &counter, .Monotonic);
    if (count != 0)
        std.debug.panic("bad counter", .{});
}

fn spawner(allocator: *std.mem.Allocator, counter: *usize) !void {
    Task.runConcurrentlyAsync();

    const frames = try allocator.alloc(@Frame(runner), num_tasks);
    defer allocator.free(frames);

    for (frames) |*frame|
        frame.* = async runner(counter);
    for (frames) |*frame|
        await frame;
}

fn runner(counter: *usize) void {
    Task.runConcurrentlyAsync();
    
    _ =  @atomicRmw(usize, counter, .Sub, 1, .Monotonic);
}
