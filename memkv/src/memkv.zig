const std = @import("std");
const Thread = std.Thread;
const Atomic = std.atomic.Atomic;
const Ordering = std.atomic.Ordering;
const Allocator = std.mem.Allocator;
const debug = std.debug;
const mem = std.mem;
const eql = std.mem.eql;
const ArrayList = std.ArrayList;

const PGSZ = mem.page_size;
const MAX_KEY_SIZE = 1 << 12 - 1;
const MAX_VALUE_SIZE = 1 << 32 - 1;

const PageNumber = enum(u64) {
    root = 0,
    _,

    fn offset(self: PageNumber) u64 {
        return @intFromEnum(self) * PGSZ;
    }
};

const TransactionId = enum(u64) { _ };

const Env = struct {
    const Self = @This();

    allocator: Allocator,

    buf: []align(PGSZ) u8,

    current_write_txn: ?*WriteTransaction,
    write_txn_available: Thread.Condition,
    write_txn_mutex: Thread.Mutex,

    next_txn_id: Atomic(u64),
    next_pgno: Atomic(u64),

    pub fn init(allocator: Allocator) !Self {
        const linux = std.os.linux;
        const length = 4096 * 1024 * 1024;
        const prot = linux.PROT.READ | linux.PROT.WRITE;
        const flags = linux.MAP.PRIVATE | linux.MAP.ANONYMOUS;
        var buf = try std.os.mmap(null, length, prot, flags, -1, 0);
        return Self{
            .allocator = allocator,
            .buf = buf,
            .current_write_txn = null,
            .write_txn_mutex = Thread.Mutex{},
            .write_txn_available = Thread.Condition{},
            .next_txn_id = Atomic(u64).init(0),
            .next_pgno = Atomic(u64).init(0),
        };
    }

    pub fn deinit(self: *Self) void {
        std.os.munmap(self.buf, self.buf.len);
    }

    pub fn begin(self: *Self) ReadTransaction {
        return ReadTransaction{ .env = self };
    }

    pub fn begin_write(self: *Self) !*WriteTransaction {
        self.write_txn_mutex.lock();
        defer self.write_txn_mutex.unlock();

        while (self.current_write_txn != null) {
            self.write_txn_available.wait(&self.write_txn_mutex);
        }

        const root_pgno = self.alloc_page(.{ .LEAF = true });
        const id: TransactionId = @enumFromInt(self.next_txn_id.fetchAdd(1, Ordering.AcqRel));

        const txn = try self.allocator.create(WriteTransaction);
        errdefer self.allocator.destroy(txn);
        txn.* = .{ .env = self, .id = id, .bt_root = root_pgno };

        self.current_write_txn = txn;
        return txn;
    }

    fn read_page(self: *Self, pgno: PageNumber) *Page {
        const offset = pgno.offset();
        return @ptrCast(@alignCast(self.buf[offset .. offset + PGSZ]));
    }

    fn alloc_page(self: *Self, flags: Page.Flags) PageNumber {
        const pgno: PageNumber = @enumFromInt(self.next_pgno.fetchAdd(1, Ordering.AcqRel));
        const offset = pgno.offset();
        const page: *Page = @ptrCast(@alignCast(&self.buf[offset]));
        page.* = .{
            .pgno = pgno,
            .flags = flags,
            .free_start = @sizeOf(Page),
            .free_end = PGSZ,
        };
        return pgno;
    }

    fn complete_write_txn(self: *Self, txn: *WriteTransaction) void {
        self.write_txn_mutex.lock();
        defer self.write_txn_mutex.unlock();

        debug.assert(self.current_write_txn == txn);

        self.current_write_txn = null;
        self.write_txn_available.signal();
    }
};

const ReadTransaction = struct {
    env: *Env,
};

const WriteTransaction = struct {
    const Self = @This();

    env: *Env,
    id: TransactionId,
    bt_root: PageNumber,

    pub fn open(self: *Self, name: []const u8) !*WriteTree {
        _ = name;
        return WriteTree.init(self);
    }

    pub fn deinit(self: *Self) void {
        defer self.env.allocator.destroy(self);
        self.env.complete_write_txn(self);
    }
};

const WriteTree = struct {
    const Self = @This();

    txn: *WriteTransaction,

    pub fn init(txn: *WriteTransaction) !*Self {
        const tree = try txn.env.allocator.create(Self);
        errdefer txn.env.allocator.destroy(tree);
        tree.* = .{ .txn = txn };
        return tree;
    }

    pub fn get(self: *Self, key: []const u8) !?[]const u8 {
        const cursor = try Cursor.init(self);
        defer cursor.deinit();
        return cursor.get(key);
    }

    pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
        const cursor = try Cursor.init(self);
        defer cursor.deinit();
        return cursor.put(key, value);
    }

    pub fn deinit(self: *Self) void {
        self.txn.env.allocator.destroy(self);
    }
};

fn ensure_key_size(key: []const u8) !void {
    if (key.len > MAX_KEY_SIZE) return error.KeyTooLarge;
}

const Cursor = struct {
    const Self = @This();

    page_stack: ArrayList(*Page),
    tree: *WriteTree,

    pub fn init(tree: *WriteTree) !*Self {
        const allocator = tree.txn.env.allocator;
        const cursor = try allocator.create(Self);
        errdefer allocator.destroy(cursor);
        var page_stack = try ArrayList(*Page).initCapacity(allocator, 4);
        const root_page = tree.txn.env.read_page(tree.txn.bt_root);
        try page_stack.append(root_page);
        cursor.* = .{
            .tree = tree,
            .page_stack = page_stack,
        };
        return cursor;
    }

    pub fn deinit(self: *Self) void {
        self.page_stack.deinit();
        self.tree.txn.env.allocator.destroy(self);
    }

    pub fn get(self: *Self, key: []const u8) !?[]const u8 {
        if (key.len > MAX_KEY_SIZE) return null;

        const entry = self.search(key);
        switch (entry) {
            .left => return null,
            .right => |ent| return ent.value(),
        }
    }

    pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
        try ensure_key_size(key);

        const entry = self.search(key);
        switch (entry) {
            .left => |slot| return self.set(slot, key, value),
            .right => return PutError.KeyExists,
        }
    }

    /// Insert a new entry into the tree at the current cursor position.
    fn set(self: *Self, slot: u16, key: []const u8, value: []const u8) !void {
        return self.current_page().add_entry(slot, key, value);
    }

    fn search(self: *Self, key: []const u8) Either(u16, *Entry) {
        var page = self.current_page();
        while (page.is_internal()) {
            unreachable;
            // const ent = page.search(key);
            // _ = ent;
            // self.page_stack.append(page);
        }
        debug.assert(page.is_leaf());
        return page.search(key);
    }

    fn current_page(self: *Self) *Page {
        return self.page_stack.getLast();
    }
};

const Entry = extern struct {
    const Self = @This();

    meta: packed struct {
        key_size: u12,
        flags: packed struct(u4) { padding: u4 = 0 },
    },
    u: extern union {
        child_pgno: PageNumber, // internal node
        data_size: u32, // leaf node
    },
    data_array: [0]u8,

    comptime {
        debug.assert(@sizeOf(Self) == 16);
        debug.assert(@alignOf(Self) == 8);
    }

    fn child_pgno(self: *const Entry) PageNumber {
        return self.u.child_pgno;
    }

    fn data_size(self: *const Entry) u32 {
        return self.u.data_size;
    }

    fn data(self: *Entry) [*]u8 {
        return @ptrCast(&self.data_array);
    }

    fn key(self: *Entry) []u8 {
        return self.data()[0..self.meta.key_size];
    }

    fn value(self: *Entry) []u8 {
        return self.data()[self.meta.key_size .. self.meta.key_size + self.data_size()];
    }
};

fn Either(comptime T: type, comptime U: type) type {
    return union(enum) {
        left: T,
        right: U,
    };
}

const Page = extern struct {
    const Self = @This();

    pgno: PageNumber,
    flags: Flags,
    free_start: Offset,
    free_end: Offset,
    /// number of allocated slots
    slot_count: u16 = 0,
    // Implicit sorted array of offsets into the page (relative to the very start of the page).
    // There is no way to do VLA like in C
    slot_array: [0]Offset = .{},

    const Offset = u16;

    comptime {
        // We rely on the assumption that the page alignment is a multiple of the entry alignment.
        debug.assert(@rem(@alignOf(Self), @alignOf(Entry)) == 0);
    }

    const Flags = packed struct(u8) {
        LEAF: bool = false,

        padding: u7 = 0,
    };

    fn is_leaf(self: *const Page) bool {
        return self.flags.LEAF;
    }

    fn is_internal(self: *const Page) bool {
        return !self.flags.LEAF;
    }

    pub fn slots(self: *Page) []Offset {
        const ptr: [*]Offset = @ptrCast(&self.slot_array);
        return ptr[0..self.slot_count];
    }

    /// Search for an entry within a page.
    /// Returns the entry with the smallest `k` where `k` >= `key`.
    fn search(self: *Page, target: []const u8) Either(u16, *Entry) {
        if (self.slot_count == 0) {
            return .{ .left = 0 };
        }

        var low: u16 = 0;
        var high: u16 = self.slot_count - 1;
        if (self.is_leaf()) {
            while (low <= high) {
                const mid = (low + high) / 2;
                const ent = self.entry(mid);
                const key = ent.key();
                switch (mem.order(u8, target, key)) {
                    .lt => high = mid - 1,
                    .eq => return .{ .right = ent },
                    .gt => low = mid + 1,
                }
            }

            return .{ .left = low };
        } else {
            // TODO
            unreachable;
        }
    }

    fn free_space(self: *const Page) u16 {
        return self.free_end - self.free_start;
    }

    fn add_entry(self: *Page, slot: u16, key: []const u8, value: []const u8) !void {
        debug.assert(key.len <= MAX_KEY_SIZE);
        debug.assert(value.len <= MAX_VALUE_SIZE);

        // allocate fresh slot
        const len = key.len + value.len;
        self.free_end -= @intCast(len);
        // ensure the entry is aligned (page is also 8 byte aligned so this works)
        self.free_end = self.free_end - @rem(self.free_end, @alignOf(Entry));
        const start = self.free_end;
        self.slot_count += 1;
        self.slots()[slot] = start;

        const ent = self.entry(slot);
        ent.meta.key_size = @intCast(key.len);
        ent.u.data_size = @intCast(value.len);
        const ptr = ent.data();

        @memcpy(ptr, key);
        @memcpy(ptr + ent.meta.key_size, value);
        // FIXME shift offsets and stuff

    }

    fn raw_entry_ptr(self: *const Page, i: u16) *const u8 {
        const slot = self.slots()[i];
        const ptr = &@as([*]u8, @ptrCast(self))[slot];
        return ptr;
    }

    fn entry(self: *Page, i: u16) *Entry {
        const slot = self.slots()[i];
        const ptr = &@as([*]u8, @ptrCast(self))[slot];
        return @ptrCast(@alignCast(ptr));
    }
};

const testing = std.testing;

fn test_put_and_get(tree: *WriteTree, key: []const u8, value: []const u8) !void {
    try tree.put(key, value);

    var v = try tree.get(key);
    try testing.expect(v != null);
    try testing.expectEqualStrings(value, v.?);
}

const PutError = error{
    KeyExists,
};

test "smoke" {
    const allocator = std.heap.c_allocator;

    testing.refAllDecls(@This());

    var env = try Env.init(allocator);
    var txn = try env.begin_write();
    defer txn.deinit();
    var tree = try txn.open("test");
    defer tree.deinit();

    try test_put_and_get(tree, "hello", "world");
    try testing.expectError(PutError.KeyExists, tree.put("hello", "world"));
    // try test_put_and_get(tree, "a", "b");
}
