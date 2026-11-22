// Shopping Sessions pipeline. See rules.md for the exact rules implemented here.
//
// Phases, each run by all threads:
//   1. links: parsed, then a lock-free union-find gives a read-only table, user ID -> person.
//   2. events and clicks: the mapped files are scanned in chunks. An event line becomes a token
//      (event ID, ingest_ts, where its record is), filed by a hash of the event ID, and, if the
//      line is valid, a record (person, ts, price, category), filed by a hash of the person. A
//      valid click becomes a record filed by person. Click duplicates are left in: copies are
//      identical, so they can change neither the credit nor the set of assisting campaigns.
//      IDs of up to 10 word characters are packed into integers. Longer ones stay bytes: a user
//      ID is interned in a lock-free table, an event ID is left in the mapped file and its
//      token points at it, a click ID is a pointer and a length.
//   3. dedup: per token partition, the copy with the smallest ingest_ts wins and every other
//      copy's record is marked dropped. An invalid winner has no record, so the event is gone.
//   4. per person partition: group by person, sort by time, sessionize, credit, count assists,
//      and write user_daily a row group at a time. campaign_daily is summed from per-thread grids.
//
// Environment, besides DATA_DIR, OUTPUT_DIR and NCPU: SS_CHUNK (bytes of input per unit of work),
// SS_PARTS and SS_TP (partition counts), SS_INTERN (slots in the string table), SS_NOWC, SS_NOTHP,
// SS_NOFAST and SS_NOMMAP switch off write combining, huge pages, the timestamp fast paths and
// input mapping. They exist for testing: every setting must give the same output.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <immintrin.h>

#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

using namespace std;
typedef uint64_t u64;
typedef int64_t i64;
typedef uint32_t u32;
typedef uint8_t u8;

static const i64 DAY_MS = 86400000;
static const i64 WEEK_MS = 7 * DAY_MS;
static const u32 GAP_MS = 1800000;
static const u32 TS_INVALID = 0xFFFFFFFFu;

[[noreturn]] static void die(const string& msg) {
    fprintf(stderr, "fatal: %s\n", msg.c_str());
    exit(2);
}

static double now_s() {
    return chrono::duration<double>(chrono::steady_clock::now().time_since_epoch()).count();
}
static double T0;
static void lap(const char* what) {
    static double last = T0;
    double t = now_s();
    fprintf(stderr, "[%7.3fs] +%.3fs %s\n", t - T0, t - last, what);
    last = t;
}

// ---------------------------------------------------------------------------
// Keys. An ID of up to 10 characters from [0-9A-Z_a-z] is packed losslessly into
// 60 bits, 6 bits a character, first character highest, codes in ASCII order: so
// two packed keys compare as integers exactly as their strings compare byte-wise.
// Any other ID gets a key with bit 63 set; such keys are compared through their bytes:
//   tag 01  interned: a canonical copy of the string, so equal strings have equal keys.
//           The low 48 bits point at the copy (a 4-byte length, then the bytes).
//   tag 10  by reference: a length and a pointer to bytes that stay readable. Equal
//           strings need not have equal keys, so only where a key is never tested for
//           equality (a click's own ID).
//   tag 00  interned in the overflow table, when the main one is crowded.
// ---------------------------------------------------------------------------
static const u64 FB = 1ull << 63;
static const u64 TAG_MASK = 3ull << 61, TAG_ARENA = 1ull << 61, TAG_REF = 2ull << 61;
static const u64 PTR47 = (1ull << 47) - 1, PTR48 = (1ull << 48) - 1;
static const size_t REF_MAXLEN = 1u << 14;
static u8 CODE[256];
static char DECODE[64];

static void init_codes() {
    int c = 1;
    for (int ch = '0'; ch <= '9'; ch++) { CODE[ch] = c; DECODE[c++] = (char)ch; }
    for (int ch = 'A'; ch <= 'Z'; ch++) { CODE[ch] = c; DECODE[c++] = (char)ch; }
    CODE[(int)'_'] = c; DECODE[c++] = '_';
    for (int ch = 'a'; ch <= 'z'; ch++) { CODE[ch] = c; DECODE[c++] = (char)ch; }
}

static inline u64 hash_bytes(const char* p, size_t n) {
    u64 h = (u64)n * 0x9E3779B97F4A7C15ull;
    while (n >= 8) {
        u64 w;
        memcpy(&w, p, 8);
        h = (h ^ w) * 0xff51afd7ed558ccdULL;
        h ^= h >> 32;
        p += 8;
        n -= 8;
    }
    if (n) {
        u64 w = 0;
        memcpy(&w, p, n);
        h = (h ^ w) * 0xff51afd7ed558ccdULL;
        h ^= h >> 32;
    }
    h *= 0xc4ceb9fe1a85ec53ULL;
    return h ^ (h >> 33);
}

// Bytes that must outlive their source: bump allocation, a thread's own, never freed.
static thread_local char* arena_cur = nullptr;
static thread_local char* arena_end = nullptr;
static inline char* arena_room(size_t n) {  // 8-byte aligned; the caller commits by moving arena_cur
    if (__builtin_expect((size_t)(arena_end - arena_cur) < n, 0)) {
        const size_t sz = max<size_t>(n, 1u << 20);
        arena_cur = (char*)malloc(sz);
        if (!arena_cur) { fprintf(stderr, "out of memory\n"); _exit(1); }
        arena_end = arena_cur + sz;
    }
    return arena_cur;
}
static inline const char* arena_copy(const char* p, size_t n) {
    char* d = arena_room((n + 7) & ~(size_t)7);
    memcpy(d, p, n);
    arena_cur = d + ((n + 7) & ~(size_t)7);
    return d;
}

// The exact string table. Lock-free: a slot is 0 or a 16-bit fingerprint over a pointer to
// a finished copy, and a slot never changes once set. A string that finds its first PROBES
// slots taken by others goes to the overflow table; those slots stay taken, so the same
// string goes there every time.
struct Intern {
    static const int NS = 64, PROBES = 32;
    struct Shard {
        mutex m;
        unordered_map<string, u32> map;
        deque<string> strs;
    } sh[NS];
    atomic<u64>* slots = nullptr;
    u64 mask = 0;
    void init(size_t want) {
        size_t n = 64;
        while (n < want) n <<= 1;
        void* m = mmap(nullptr, n * 8, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (m == MAP_FAILED) { fprintf(stderr, "out of memory\n"); _exit(1); }
        madvise(m, n * 8, MADV_HUGEPAGE);
        slots = (atomic<u64>*)m;
        mask = n - 1;
    }
    u64 overflow(const char* p, size_t n, u64 h) {
        string s(p, n);
        Shard& S = sh[h % NS];
        lock_guard<mutex> g(S.m);
        auto it = S.map.find(s);
        u32 id;
        if (it == S.map.end()) {
            id = (u32)S.strs.size();
            S.strs.push_back(s);
            S.map.emplace(std::move(s), id);
        } else {
            id = it->second;
        }
        return FB | ((u64)id << 6) | (h % NS);
    }
    u64 get(const char* p, size_t n) {
        const u64 h = hash_bytes(p, n);
        if (__builtin_expect(n > 0xFFFFFFF0u, 0)) return overflow(p, n, h);
        const u64 fp = (h >> 48) << 48;
        char* rec = nullptr;
        size_t i = (size_t)(h & mask);
        for (int probe = 0; probe < PROBES; probe++, i = (i + 1) & mask) {
            u64 v = slots[i].load(memory_order_acquire);
            if (!v) {
                if (!rec) {
                    rec = arena_room((n + 4 + 7) & ~(size_t)7);
                    const u32 len = (u32)n;
                    memcpy(rec, &len, 4);
                    memcpy(rec + 4, p, n);
                }
                if ((u64)(uintptr_t)rec > PTR48) return overflow(p, n, h);
                if (slots[i].compare_exchange_strong(v, fp | (u64)(uintptr_t)rec, memory_order_acq_rel)) {
                    arena_cur = rec + ((n + 4 + 7) & ~(size_t)7);
                    return FB | TAG_ARENA | (u64)(uintptr_t)rec;
                }
            }
            if ((v >> 48 << 48) == fp) {
                const char* r = (const char*)(uintptr_t)(v & PTR48);
                u32 len;
                memcpy(&len, r, 4);
                if (len == n && memcmp(r + 4, p, n) == 0) return FB | TAG_ARENA | (v & PTR48);
            }
        }
        return overflow(p, n, h);
    }
    string_view str(u64 key) {
        if ((key & TAG_MASK) == TAG_ARENA) {
            const char* r = (const char*)(uintptr_t)(key & PTR48);
            u32 len;
            memcpy(&len, r, 4);
            return string_view(r + 4, len);
        }
        if ((key & TAG_MASK) == TAG_REF) return string_view((const char*)(uintptr_t)(key & PTR47), (size_t)((key >> 47) & (REF_MAXLEN - 1)));
        Shard& S = sh[key & 63];
        lock_guard<mutex> g(S.m);
        return S.strs[(key & ~FB) >> 6];  // deque elements never move
    }
};
static Intern intern;

static vector<string> cat_names;
static vector<u32> cat_rank;
static int CAT_UNKNOWN;

alignas(16) static u8 LENMASK[17][16];  // row n: the first n bytes set
alignas(16) static u8 SHUF_R[17][16];   // row n: moves bytes 0..n-1 to the last n places, zero before them

static void init_simd_tables() {
    for (int n = 0; n <= 16; n++)
        for (int i = 0; i < 16; i++) {
            LENMASK[n][i] = i < n ? 0xFF : 0;
            SHUF_R[n][i] = i >= 16 - n ? (u8)(i - (16 - n)) : 0x80;
        }
}

// p must be readable for 16 bytes; n >= 1.
static inline u64 pack(const char* p, size_t n) {
    if (__builtin_expect(n <= 10, 1)) {
        // A byte is in the alphabet when the class bits of its two nibbles meet.
        const __m128i lut_hi = _mm_setr_epi8(0, 0, 0, 1, 2, 12, 2, 4, 0, 0, 0, 0, 0, 0, 0, 0);
        const __m128i lut_lo = _mm_setr_epi8(5, 7, 7, 7, 7, 7, 7, 7, 7, 7, 6, 2, 2, 2, 2, 10);
        const __m128i off_hi = _mm_setr_epi8(0, 0, 0, -0x2F, -0x36, -0x36, -0x3B, -0x3B, 0, 0, 0, 0, 0, 0, 0, 0);
        const __m128i lm = _mm_load_si128((const __m128i*)LENMASK[n]);
        const __m128i x = _mm_and_si128(_mm_loadu_si128((const __m128i*)p), lm);
        const __m128i hi = _mm_and_si128(_mm_srli_epi16(x, 4), _mm_set1_epi8(0x0F));
        const __m128i lo = _mm_and_si128(x, _mm_set1_epi8(0x0F));
        const __m128i ok = _mm_and_si128(_mm_shuffle_epi8(lut_hi, hi), _mm_shuffle_epi8(lut_lo, lo));
        const __m128i bad = _mm_and_si128(_mm_cmpeq_epi8(ok, _mm_setzero_si128()), lm);
        if (__builtin_expect(_mm_testz_si128(bad, bad), 1)) {
            __m128i c = _mm_add_epi8(x, _mm_shuffle_epi8(off_hi, hi));
            c = _mm_add_epi8(c, _mm_and_si128(_mm_cmpeq_epi8(x, _mm_set1_epi8('_')), _mm_set1_epi8(-4)));
            const __m128i w = _mm_maddubs_epi16(c, _mm_set1_epi16(0x0140));     // two codes -> 12 bits
            const __m128i d = _mm_madd_epi16(w, _mm_set1_epi32(0x00011000));    // four codes -> 24 bits
            const u64 d01 = (u64)_mm_cvtsi128_si64(d);
            const u64 d2 = (u32)_mm_extract_epi32(d, 2);
            return ((d01 & 0xFFFFFFFFull) << 36) | ((d01 >> 32) << 12) | (d2 >> 12);
        }
    }
    return intern.get(p, n);
}

// Two IDs in one pass. Bit 0 / bit 1 of the result says the first / second ID was packed into
// k0 / k1; an ID that is empty, longer than 10 or outside the alphabet is left to pack().
// Both pointers must be readable for 16 bytes.
static inline __attribute__((always_inline)) unsigned pack2(const char* p0, size_t n0, const char* p1, size_t n1, u64& k0,
                                                             u64& k1) {
    const size_t m0 = n0 <= 10 ? n0 : 0, m1 = n1 <= 10 ? n1 : 0;
    const __m256i lut_hi = _mm256_setr_epi8(0, 0, 0, 1, 2, 12, 2, 4, 0, 0, 0, 0, 0, 0, 0, 0,
                                            0, 0, 0, 1, 2, 12, 2, 4, 0, 0, 0, 0, 0, 0, 0, 0);
    const __m256i lut_lo = _mm256_setr_epi8(5, 7, 7, 7, 7, 7, 7, 7, 7, 7, 6, 2, 2, 2, 2, 10,
                                            5, 7, 7, 7, 7, 7, 7, 7, 7, 7, 6, 2, 2, 2, 2, 10);
    const __m256i off_hi = _mm256_setr_epi8(0, 0, 0, -0x2F, -0x36, -0x36, -0x3B, -0x3B, 0, 0, 0, 0, 0, 0, 0, 0,
                                            0, 0, 0, -0x2F, -0x36, -0x36, -0x3B, -0x3B, 0, 0, 0, 0, 0, 0, 0, 0);
    const __m256i lm = _mm256_inserti128_si256(_mm256_castsi128_si256(_mm_load_si128((const __m128i*)LENMASK[m0])),
                                               _mm_load_si128((const __m128i*)LENMASK[m1]), 1);
    const __m256i raw = _mm256_inserti128_si256(_mm256_castsi128_si256(_mm_loadu_si128((const __m128i*)p0)),
                                                _mm_loadu_si128((const __m128i*)p1), 1);
    const __m256i x = _mm256_and_si256(raw, lm);
    const __m256i hi = _mm256_and_si256(_mm256_srli_epi16(x, 4), _mm256_set1_epi8(0x0F));
    const __m256i lo = _mm256_and_si256(x, _mm256_set1_epi8(0x0F));
    const __m256i ok = _mm256_and_si256(_mm256_shuffle_epi8(lut_hi, hi), _mm256_shuffle_epi8(lut_lo, lo));
    const u32 bad = (u32)_mm256_movemask_epi8(_mm256_and_si256(_mm256_cmpeq_epi8(ok, _mm256_setzero_si256()), lm));
    __m256i c = _mm256_add_epi8(x, _mm256_shuffle_epi8(off_hi, hi));
    c = _mm256_add_epi8(c, _mm256_and_si256(_mm256_cmpeq_epi8(x, _mm256_set1_epi8('_')), _mm256_set1_epi8(-4)));
    const __m256i w = _mm256_maddubs_epi16(c, _mm256_set1_epi16(0x0140));
    const __m256i d = _mm256_madd_epi16(w, _mm256_set1_epi32(0x00011000));
    const __m128i da = _mm256_castsi256_si128(d), db = _mm256_extracti128_si256(d, 1);
    const u64 a01 = (u64)_mm_cvtsi128_si64(da), a2 = (u32)_mm_extract_epi32(da, 2);
    const u64 b01 = (u64)_mm_cvtsi128_si64(db), b2 = (u32)_mm_extract_epi32(db, 2);
    k0 = ((a01 & 0xFFFFFFFFull) << 36) | ((a01 >> 32) << 12) | (a2 >> 12);
    k1 = ((b01 & 0xFFFFFFFFull) << 36) | ((b01 >> 32) << 12) | (b2 >> 12);
    return (unsigned)(m0 != 0 && !(bad & 0xFFFF)) | (unsigned)(m1 != 0 && !(bad >> 16)) << 1;
}

// Writes the bytes of a key to out (at least 10 bytes of room for packed keys).
static inline string_view key_str(u64 k, char* tmp) {
    if (k & FB) return intern.str(k);
    int n = 0;
    for (int s = 54; s >= 0; s -= 6) {
        unsigned c = (k >> s) & 63;
        if (!c) break;
        tmp[n++] = DECODE[c];
    }
    return string_view(tmp, n);
}

// Byte-wise order of the IDs behind two keys.
static inline bool key_less(u64 a, u64 b) {
    if (!((a | b) & FB)) return a < b;
    char ta[16], tb[16];
    return key_str(a, ta) < key_str(b, tb);  // string_view compares bytes as unsigned char
}

static inline u64 mix(u64 x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

// Open addressing u64 -> u64 map; key 0 is never a valid key.
struct KMap {
    struct E { u64 k, v; };
    vector<E> t;
    u64 mask = 0;
    void init(size_t n) {
        size_t cap = 16;
        while (cap < n * 2 + 2) cap <<= 1;
        t.assign(cap, E{0, 0});
        mask = cap - 1;
    }
    // Returns the slot for k, inserting it with value dflt if absent.
    inline u64& at(u64 k, u64 dflt) {
        u64 i = mix(k) >> 20 & mask;
        for (;;) {
            if (t[i].k == k) return t[i].v;
            if (t[i].k == 0) { t[i].k = k; t[i].v = dflt; return t[i].v; }
            i = (i + 1) & mask;
        }
    }
    inline const u64* find(u64 k) const {
        u64 i = mix(k) >> 20 & mask;
        for (;;) {
            if (t[i].k == k) return &t[i].v;
            if (t[i].k == 0) return nullptr;
            i = (i + 1) & mask;
        }
    }
};

// ---------------------------------------------------------------------------
// Timestamps
// ---------------------------------------------------------------------------
static inline i64 days_from_civil(i64 y, i64 m, i64 d) {
    y -= m <= 2;
    i64 era = (y >= 0 ? y : y - 399) / 400;
    i64 yoe = y - era * 400;
    i64 doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    i64 doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}

static inline bool d2(const char* p, int& v) {
    unsigned a = (u8)p[0] - '0', b = (u8)p[1] - '0';
    if (a > 9 || b > 9) return false;
    v = (int)(a * 10 + b);
    return true;
}

// p points at 19 characters YYYY-MM-DDTHH:MM:SS
static inline bool parse_civil(const char* p, i64& sec) {
    if (p[4] != '-' || p[7] != '-' || p[10] != 'T' || p[13] != ':' || p[16] != ':') return false;
    int y1, y2, mo, d, h, mi, s;
    if (!d2(p, y1) || !d2(p + 2, y2) || !d2(p + 5, mo) || !d2(p + 8, d) || !d2(p + 11, h) ||
        !d2(p + 14, mi) || !d2(p + 17, s))
        return false;
    int y = y1 * 100 + y2;
    if (mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || s > 59) return false;
    static const int dim[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int md = dim[mo - 1];
    if (mo == 2 && (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0))) md = 29;
    if (d > md) return false;
    sec = days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + s;
    return true;
}

static const i64 TS_FAR = INT64_MAX / 4;  // a valid number that is outside every window

// Returns false for a bad form. On true, ms is the instant (TS_FAR if too large to matter).
static __attribute__((noinline)) bool parse_ts(const char* p, size_t n, i64& ms) {
    if (n == 20 && p[19] == 'Z') {
        i64 sec;
        if (!parse_civil(p, sec)) return false;
        ms = sec * 1000;
        return true;
    }
    if (n == 25 && (p[19] == '+' || p[19] == '-')) {
        i64 sec;
        int oh, om;
        if (p[22] != ':' || !d2(p + 20, oh) || !d2(p + 23, om) || oh > 23 || om > 59) return false;
        if (!parse_civil(p, sec)) return false;
        i64 off = (oh * 60 + om) * 60;
        ms = (p[19] == '+' ? sec - off : sec + off) * 1000;
        return true;
    }
    if (n == 0) return false;
    for (size_t i = 0; i < n; i++)
        if ((unsigned)((u8)p[i] - '0') > 9) return false;
    size_t i = 0;
    while (i < n && p[i] == '0') i++;
    if (n - i > 16) { ms = TS_FAR; return true; }
    i64 v = 0;
    for (; i < n; i++) v = v * 10 + (p[i] - '0');
    ms = v;
    return true;
}

// ---- Fast paths. They accept exactly what parse_ts accepts, for the timestamps that matter.
static inline void civil_from_days(i64 z, i64& y, unsigned& m, unsigned& d) {
    z += 719468;
    i64 era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = (unsigned)(z - era * 146097);
    unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    y = (i64)yoe + era * 400;
    unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    unsigned mp = (5 * doy + 2) / 153;
    d = doy - (153 * mp + 2) / 5 + 1;
    m = mp < 10 ? mp + 3 : mp - 9;
    y += (m <= 2);
}

// Every calendar day a timestamp of interest can show, keyed by its ten characters: from the
// day before the click window (an offset can move the local date a day either way) to a week
// past the event window (late ingests). A date that is not here is outside both windows.
struct DateEnt { u64 a; u32 b; int32_t day; };
static DateEnt DATES[26 * 64];
static bool DATES_OK = false;

static inline unsigned date_slot(const char* p) {
    return (((u8)p[5] & 1) * 10 + ((u8)p[6] & 15)) * 64 + ((u8)p[8] & 3) * 10 + ((u8)p[9] & 15);
}

// p: 19 characters. 0: valid, sec set. 1: malformed. 2: a date the table does not hold.
static inline __attribute__((always_inline)) int fast_civil(const char* p, i64& sec) {
    u64 a;
    uint16_t b;
    memcpy(&a, p, 8);
    memcpy(&b, p + 8, 2);
    const DateEnt& e = DATES[date_slot(p)];
    if (e.a != a || e.b != ((u32)b | 0x10000)) return 2;
    u64 t;
    memcpy(&t, p + 11, 8);  // HH:MM:SS
    const u64 x = t ^ 0x30303A30303A3030ull;
    // every digit place at most 9, both colon places exactly ':'
    if (p[10] != 'T' || ((((x & 0x7F7F7F7F7F7F7F7Full) + 0x76767F76767F7676ull) | x) & 0x8080808080808080ull)) return 1;
    const unsigned h = (unsigned)(x & 15) * 10 + (unsigned)(x >> 8 & 15);
    const unsigned mi = (unsigned)(x >> 24 & 15) * 10 + (unsigned)(x >> 32 & 15);
    const unsigned s = (unsigned)(x >> 48 & 15) * 10 + (unsigned)(x >> 56 & 15);
    if (h > 23 || mi > 59 || s > 59) return 1;
    sec = (i64)e.day * 86400 + h * 3600 + mi * 60 + s;
    return 0;
}

// Digits only, 1 <= n <= 16; p readable for 16 bytes.
static inline __attribute__((always_inline)) bool parse_f3_16(const char* p, size_t n, i64& ms) {
    const __m128i lm = _mm_load_si128((const __m128i*)LENMASK[n]);
    __m128i d = _mm_sub_epi8(_mm_loadu_si128((const __m128i*)p), _mm_set1_epi8('0'));
    const __m128i ok = _mm_cmpeq_epi8(_mm_max_epu8(d, _mm_set1_epi8(9)), _mm_set1_epi8(9));
    if (!_mm_testc_si128(ok, lm)) return false;
    d = _mm_shuffle_epi8(d, _mm_load_si128((const __m128i*)SHUF_R[n]));
    __m128i t = _mm_maddubs_epi16(d, _mm_set1_epi16(0x010A));
    t = _mm_madd_epi16(t, _mm_set1_epi32(0x00010064));
    t = _mm_packus_epi32(t, t);
    t = _mm_madd_epi16(t, _mm_set1_epi32(0x00012710));
    const u64 v = (u64)_mm_cvtsi128_si64(t);
    ms = (i64)((v & 0xFFFFFFFFull) * 100000000ull + (v >> 32));
    return true;
}

// For a ts that is only kept if it lies in a window: true with ms set for every valid timestamp
// in the date table's range; outside that range it may answer false for a valid one.
static inline __attribute__((always_inline)) bool parse_ts_win(const char* p, size_t n, i64& ms) {
    if (n == 20) {
        if (p[19] == 'Z' && DATES_OK) {
            i64 sec;
            if (fast_civil(p, sec)) return false;
            ms = sec * 1000;
            return true;
        }
        return parse_ts(p, n, ms);
    }
    if (n == 25) {
        if ((p[19] == '+' || p[19] == '-') && DATES_OK) {
            i64 sec;
            int oh, om;
            if (fast_civil(p, sec)) return false;
            if (p[22] != ':' || !d2(p + 20, oh) || !d2(p + 23, om) || oh > 23 || om > 59) return false;
            const i64 off = (oh * 60 + om) * 60;
            ms = (p[19] == '+' ? sec - off : sec + off) * 1000;
            return true;
        }
        return parse_ts(p, n, ms);
    }
    if (n == 0) return false;
    if (n <= 16) return parse_f3_16(p, n, ms);
    return parse_ts(p, n, ms);
}

// For ingest_ts, which is needed whatever its date: exactly parse_ts.
static inline __attribute__((always_inline)) bool parse_ts_any(const char* p, size_t n, i64& ms) {
    if (n == 20 && p[19] == 'Z' && DATES_OK) {
        i64 sec;
        int r = fast_civil(p, sec);
        if (r == 0) { ms = sec * 1000; return true; }
        if (r == 1) return false;
    }
    return parse_ts(p, n, ms);
}

// ---------------------------------------------------------------------------
// CSV: lines end at \n only; every comma separates fields; a quote is legal only
// as the first and last character of a field.
// ---------------------------------------------------------------------------
// [base, base + len) is a run of whole lines, len >= 1, whose last byte is '\n'. base must be
// readable from one byte before it to 64 bytes past the run, and fields may be read 32 bytes
// past their end.
//
// A line is well formed when every field either holds no quote or is at least two characters
// long with quotes exactly at its two ends. Counting does that check: if the quotes of a line
// number twice its fields that begin and end with one, no quote is anywhere else.
template <int NF, class H>
static inline void scan_lines(const char* base, size_t len, H&& h) {
    const __m256i vc = _mm256_set1_epi8(','), vn = _mm256_set1_epi8('\n'), vq = _mm256_set1_epi8('"');
    const char* fs[NF];
    u32 fl[NF];
    size_t wi = 0, fpos = 0;
    u64 cur, ncur, qcur;  // delimiters, newlines and quotes of 64-byte word wi, minus those consumed
    auto load = [&](size_t w) __attribute__((always_inline)) {
        const __m256i x0 = _mm256_loadu_si256((const __m256i*)(base + w * 64));
        const __m256i x1 = _mm256_loadu_si256((const __m256i*)(base + w * 64 + 32));
        const u64 n = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x0, vn)) |
                      (u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x1, vn)) << 32;
        const u64 c = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x0, vc)) |
                      (u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x1, vc)) << 32;
        qcur = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x0, vq)) |
               (u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x1, vq)) << 32;
        ncur = n;
        cur = n | c;
    };
    load(0);
    while (fpos < len) {
        int nf = 0;
        unsigned looks_quoted = 0, quotes = 0;
        for (;;) {
            while (!cur) {  // the run ends with a newline, so a delimiter is always ahead
                quotes += (unsigned)__builtin_popcountll(qcur);
                load(++wi);
            }
            const u64 bit = cur & (0 - cur);
            const size_t d = wi * 64 + (size_t)__builtin_ctzll(cur);
            cur ^= bit;
            if (nf < NF) {
                const size_t n = d - fpos;
                const unsigned q = (unsigned)(n >= 2) & (unsigned)(base[fpos] == '"') & (unsigned)(base[d - 1] == '"');
                fs[nf] = base + fpos + q;
                fl[nf] = (u32)(n - 2 * q);
                looks_quoted += q;
            }
            nf++;
            fpos = d + 1;
            if (ncur & bit) {
                const u64 upto = bit | (bit - 1);
                quotes += (unsigned)__builtin_popcountll(qcur & upto);
                qcur &= ~upto;
                break;
            }
        }
        if (nf == NF && quotes == 2 * looks_quoted) h(fs, fl);
    }
}

static bool USE_MMAP = true;  // off: read the inputs into memory, the path taken when a file cannot be mapped

// An input file, mapped. p is readable up to `mapped`, at least `size`.
struct MFile { const char* p = nullptr; size_t size = 0, mapped = 0; bool is_map = false; };

static MFile map_file(const string& path) {
    MFile f;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) die("cannot open " + path);
    struct stat st;
    if (fstat(fd, &st) != 0) die("cannot stat " + path);
    f.size = (size_t)st.st_size;
    if (f.size) {
        void* m = USE_MMAP ? mmap(nullptr, f.size, PROT_READ, MAP_PRIVATE, fd, 0) : MAP_FAILED;
        if (m != MAP_FAILED) {
            f.p = (const char*)m;
            f.mapped = (f.size + 4095) & ~(size_t)4095;
            f.is_map = true;
        } else {  // a filesystem that cannot be mapped: read it
            char* buf = (char*)malloc(f.size + 128);
            if (!buf) die("out of memory reading " + path);
            size_t have = 0;
            while (have < f.size) {
                ssize_t r = pread(fd, buf + have, f.size - have, (off_t)have);
                if (r < 0) { if (errno == EINTR) continue; die("read failed: " + path); }
                if (r == 0) break;
                have += (size_t)r;
            }
            f.size = have;
            memset(buf + have, 0, 128);
            f.p = buf;
            f.mapped = have + 128;
        }
    }
    close(fd);
    return f;
}

// Handles the lines of a file that start in [a, b). The first line of the file is the header.
// Lines are scanned in place, except those so near the end of the mapping that a 64-byte read
// could leave it, and a last line with no newline: those are copied to `tail`, padded.
template <int NF, class H>
static void process_chunk(const MFile& f, u64 a, u64 b, vector<char>& tail, H&& h) {
    const size_t S = f.size;
    if (b > S) b = S;
    if (a >= b) return;
    const char* m = f.p;
    // Lines start after the first newline at or after a-1: for a == 0 that skips the header,
    // otherwise it finds the first line starting at or after a.
    const size_t rs = a ? a - 1 : 0;
    const char* nl = (const char*)memchr(m + rs, '\n', S - rs);
    if (!nl) return;
    const size_t start = (size_t)(nl - m) + 1;
    if (start >= b) return;
    // The last line of this chunk is the one holding byte b-1.
    const size_t from = max<size_t>(start, (size_t)b - 1);
    const char* e = (const char*)memchr(m + from, '\n', S - from);
    const size_t end = e ? (size_t)(e - m) + 1 : S;
    const size_t safe = f.mapped >= 64 ? f.mapped - 64 : 0;
    size_t cut = end;  // [start, cut) in place, [cut, end) through the copy
    if (!e || end > safe) {
        const size_t lim = min(end, safe);
        cut = start;
        if (lim > start) {
            const char* r = (const char*)memrchr(m + start, '\n', lim - start);
            if (r) cut = (size_t)(r - m) + 1;
        }
    }
    if (cut > start) scan_lines<NF>(m + start, cut - start, h);
    if (cut < end) {
        size_t n = end - cut;
        if (tail.size() < n + 256) tail.resize(n + 256);
        char* t = tail.data();
        memset(t, 0, 64);
        memcpy(t + 64, m + cut, n);
        if (!e) t[64 + n++] = '\n';
        memset(t + 64 + n, 0, 128);
        scan_lines<NF>(t + 64, n, h);
    }
}

// ---------------------------------------------------------------------------
// Parquet writer: one data page per column chunk, PLAIN, uncompressed.
// ---------------------------------------------------------------------------
struct Thrift {
    string b;
    vector<int> stack;
    int last = 0;
    void varint(u64 v) {
        while (v >= 0x80) { b.push_back((char)(v | 0x80)); v >>= 7; }
        b.push_back((char)v);
    }
    void zz(i64 v) { varint(((u64)v << 1) ^ (u64)(v >> 63)); }
    void field(int id, int type) {
        int d = id - last;
        if (d > 0 && d <= 15) b.push_back((char)((d << 4) | type));
        else { b.push_back((char)type); zz(id); }
        last = id;
    }
    void i32f(int id, i64 v) { field(id, 5); zz(v); }
    void i64f(int id, i64 v) { field(id, 6); zz(v); }
    void str(const string& s) { varint(s.size()); b += s; }
    void strf(int id, const string& s) { field(id, 8); str(s); }
    void list(int id, int elem, size_t n) {
        field(id, 9);
        if (n < 15) b.push_back((char)((n << 4) | elem));
        else { b.push_back((char)(0xF0 | elem)); varint(n); }
    }
    void begin() { stack.push_back(last); last = 0; }             // a struct as a list element
    void sbegin(int id) { field(id, 12); begin(); }               // a struct as a field
    void end() { b.push_back(0); last = stack.back(); stack.pop_back(); }
};

enum ColKind { C_STR, C_STR_OPT, C_DATE, C_I32, C_I64 };
struct ColSpec { const char* name; ColKind kind; };
static inline int phys_type(ColKind k) { return k == C_I64 ? 2 : (k == C_DATE || k == C_I32) ? 1 : 6; }

// Row groups go to disk as they are handed over, so the rows never sit in memory twice.
struct PqFile {
    vector<ColSpec> cols;
    string path;
    int fd = -1;
    u64 off = 0, total_rows = 0;
    struct RG { u64 rows; vector<pair<u64, u64>> chunks; };  // (offset, size) of each column chunk
    vector<RG> rgs;

    void create(const string& p, vector<ColSpec> c) {
        path = p;
        cols = std::move(c);
        fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) die("cannot create " + path);
        struct iovec v = {(void*)"PAR1", 4};
        put(&v, 1);
    }
    void put(struct iovec* iov, int cnt) {
        while (cnt) {
            ssize_t w = writev(fd, iov, cnt);
            if (w < 0) { if (errno == EINTR) continue; die("write failed: " + path); }
            off += (u64)w;
            while (cnt && (size_t)w >= iov->iov_len) { w -= (ssize_t)iov->iov_len; iov++; cnt--; }
            if (cnt) { iov->iov_base = (char*)iov->iov_base + w; iov->iov_len -= (size_t)w; }
        }
    }
    // pages[i] is the page payload of column i: (def levels for optional) + PLAIN values.
    void group(u64 rows, const string* pages) {
        const size_t nc = cols.size();
        vector<string> hdr(nc);
        vector<struct iovec> iov(2 * nc);
        RG rg;
        rg.rows = rows;
        u64 o = off;
        for (size_t i = 0; i < nc; i++) {
            Thrift h;
            h.i32f(1, 0);                         // DATA_PAGE
            h.i32f(2, (i64)pages[i].size());
            h.i32f(3, (i64)pages[i].size());
            h.sbegin(5);
            h.i32f(1, (i64)rows);
            h.i32f(2, 0);                         // PLAIN
            h.i32f(3, 3);                         // RLE
            h.i32f(4, 3);                         // RLE
            h.end();
            h.b.push_back(0);
            hdr[i] = std::move(h.b);
            const u64 size = hdr[i].size() + pages[i].size();
            rg.chunks.push_back({o, size});
            o += size;
            iov[2 * i] = {(void*)hdr[i].data(), hdr[i].size()};
            iov[2 * i + 1] = {(void*)pages[i].data(), pages[i].size()};
        }
        put(iov.data(), (int)iov.size());
        rgs.push_back(std::move(rg));
        total_rows += rows;
    }
    void finish() {
        Thrift m;
        m.i32f(1, 1);
        m.list(2, 12, cols.size() + 1);
        m.begin();
        m.strf(4, "schema");
        m.i32f(5, (i64)cols.size());
        m.end();
        for (auto& c : cols) {
            m.begin();
            m.i32f(1, phys_type(c.kind));
            m.i32f(3, c.kind == C_STR_OPT ? 1 : 0);
            m.strf(4, c.name);
            if (c.kind == C_STR || c.kind == C_STR_OPT) {
                m.i32f(6, 0);  // UTF8
                m.sbegin(10); m.sbegin(1); m.end(); m.end();
            } else if (c.kind == C_DATE) {
                m.i32f(6, 6);  // DATE
                m.sbegin(10); m.sbegin(6); m.end(); m.end();
            }
            m.end();
        }
        m.i64f(3, (i64)total_rows);
        m.list(4, 12, rgs.size());
        for (auto& rg : rgs) {
            m.begin();
            m.list(1, 12, cols.size());
            u64 total = 0;
            for (size_t i = 0; i < cols.size(); i++) {
                const auto& ch = rg.chunks[i];
                total += ch.second;
                m.begin();
                m.i64f(2, (i64)ch.first);
                m.sbegin(3);
                m.i32f(1, phys_type(cols[i].kind));
                m.list(2, 5, 2); m.zz(0); m.zz(3);
                m.list(3, 8, 1); m.str(cols[i].name);
                m.i32f(4, 0);
                m.i64f(5, (i64)rg.rows);
                m.i64f(6, (i64)ch.second);
                m.i64f(7, (i64)ch.second);
                m.i64f(9, (i64)ch.first);
                m.end();
                m.end();
            }
            m.i64f(2, (i64)total);
            m.i64f(3, (i64)rg.rows);
            m.end();
        }
        m.strf(6, "shopping-sessions-cpp");
        m.b.push_back(0);
        u32 len = (u32)m.b.size();
        m.b.append((const char*)&len, 4);
        m.b += "PAR1";
        struct iovec v = {(void*)m.b.data(), m.b.size()};
        put(&v, 1);
        close(fd);
        fd = -1;
    }
};

static inline void put32(string& s, u32 v) { s.append((const char*)&v, 4); }
static inline void put64(string& s, u64 v) { s.append((const char*)&v, 8); }
static inline void put_str(string& s, string_view v) { put32(s, (u32)v.size()); s.append(v.data(), v.size()); }

// Definition levels (bit width 1) for a data page v1: 4-byte length + one bit-packed run.
static string def_levels(const vector<u8>& present) {
    size_t n = present.size(), groups = (n + 7) / 8;
    Thrift t;
    t.varint((groups << 1) | 1);
    string bits(groups, '\0');
    for (size_t i = 0; i < n; i++)
        if (present[i]) bits[i >> 3] |= (char)(1 << (i & 7));
    string out;
    put32(out, (u32)(t.b.size() + bits.size()));
    out += t.b;
    out += bits;
    return out;
}

// ---------------------------------------------------------------------------
// Pipeline state
// ---------------------------------------------------------------------------
static int NT = 1;
static i64 WS, WE, BASE;         // window start/end, BASE = WS - 7 days: all stored ts are relative to it
static i64 DAY_BASE, DAY_OFF;    // day number of BASE and BASE's offset into that day
static int NDAYS;
static i64 ING_BASE;             // seconds; a token keeps ingest_ts as u32 seconds past this
static KMap products, campaigns;
// Most product and campaign IDs are a short prefix and a number. Written without a leading zero,
// such an ID is named by the number alone, so an array indexed by it finds it at once. An ID
// the array does not hold, of any shape, is looked up in the hash table.
static vector<u8> prod_direct;         // category by product number; 0xFF: no such product
static vector<uint16_t> camp_direct;   // campaign index by number; 0xFFFF: no such campaign
static const u32 PROD_DIRECT_MAX = 1u << 22, CAMP_DIRECT_MAX = 1u << 16;
static vector<string> camp_ids, camp_channels;
static int NCAMP;  // index NCAMP is organic
static bool USE_THP = true;
static bool USE_WC = true;       // tokens and records go through write-combining lines and non-temporal stores

static i64 floor_div(i64 a, i64 b) { return a >= 0 ? a / b : -((-a + b - 1) / b); }

// ---- Workers: thread 0 is the caller, the others sleep between phases.
struct Pool {
    int n = 1;
    vector<thread> th;
    atomic<u32> gen{0};
    atomic<int> pending{0};
    function<void(int)> job;
    void start(int nt) {
        n = nt;
        for (int t = 1; t < n; t++)
            th.emplace_back([this, t] {
                u32 seen = 0;
                for (;;) {
                    gen.wait(seen);
                    seen = gen.load();
                    job(t);
                    if (pending.fetch_sub(1) == 1) pending.notify_one();
                }
            });
    }
    template <class F>
    void run(F&& f) {
        job = [&f](int t) { f(t); };
        pending.store(n - 1);
        gen.fetch_add(1);
        gen.notify_all();
        f(0);
        for (int p; (p = pending.load()) != 0;) pending.wait(p);
    }
};
static Pool pool;

// ---- Arena: 2 MB slabs cut into 4 KB blocks. A 32-bit address names a 16-byte unit,
// slab << 17 | unit, so a record can be pointed at from a 16-byte token.
static const size_t SLAB = 2u << 20, BLOCK = 4096;
static const u32 NOADDR = 0xFFFFFFFFu;
static char* slab_base[32768];
static u32 slab_count = 0;
static mutex slab_mu;
static char* chunk_cur = nullptr;
static char* chunk_end = nullptr;

static void* big_alloc(size_t bytes) {  // zeroed, 2 MB aligned, huge pages if the system allows
    char* m = (char*)mmap(nullptr, bytes + SLAB, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (m == MAP_FAILED) die("out of memory");
    char* al = (char*)(((uintptr_t)m + SLAB - 1) & ~(uintptr_t)(SLAB - 1));
    if (USE_THP) madvise(al, bytes, MADV_HUGEPAGE);
    return al;
}

static char* new_slab(u32& idx) {
    lock_guard<mutex> g(slab_mu);
    if (chunk_cur == chunk_end) {
        const size_t sz = 64u << 20;
        chunk_cur = (char*)big_alloc(sz);
        chunk_end = chunk_cur + sz;
    }
    if (slab_count >= 32768) die("arena exhausted");
    idx = slab_count++;
    slab_base[idx] = chunk_cur;
    chunk_cur += SLAB;
    return slab_base[idx];
}
static inline char* addr_ptr(u32 a) { return slab_base[a >> 17] + ((size_t)(a & 0x1FFFF) << 4); }

struct Alloc {
    char* cur = nullptr;
    char* end = nullptr;
    u32 slab = 0;
    char* block(u32& addr) {
        if (cur == end) { cur = new_slab(slab); end = cur + SLAB; }
        addr = slab << 17 | (u32)((cur - slab_base[slab]) >> 4);
        char* b = cur;
        cur += BLOCK;
        return b;
    }
};

// ---- What flows between the stages
// A token is one parsed event line: enough to pick the copy that survives the dedup.
// ing is ingest_ts in whole seconds past ING_BASE, clamped to 32 bits. It orders copies exactly
// as the timestamps do while ingest_ts is whole seconds (the rules: always F1) within about
// 34 years before and 100 after the window; beyond either, copies would tie.
struct Tok { u64 ev; u32 ing; u32 addr; };   // addr: the event's record, NOADDR for an invalid copy
// ev is the packed event ID, or for an ID that does not pack: bit 63, 27 bits of a hash of the
// bytes, the thread that saw it (8 bits) and its index in that thread's `side` (28 bits). The
// hash bits file and find such a token; the bytes decide whether two of them are one event.
static const u64 EV_LOC = (1ull << 36) - 1;
static inline u64 ev_key(u64 ev) { return ev & ~((u64)((i64)ev >> 63) & EV_LOC); }
struct PEv { u64 person; u64 w; };           // a valid event: w = ts << 32 | price << 8 | category; ~0 once dropped
struct Clk { u64 user; u64 id; u32 ts; u32 camp; };  // a valid click; user is the person
struct CItem { u64 tc; u64 id; };             // a click inside the person stage: tc = ts << 32 | campaign
struct AggHot { u32 sessions, assisted; };    // per (campaign, day); touched for every session
struct AggCold { u64 revenue; u64 purchasing; };

static inline bool alive(const PEv& r) { return (r.w >> 32) != 0xFFFFFFFFull; }
static inline bool alive(const Clk&) { return true; }

enum { K_TOK = 0, K_REC = 1, K_CLK = 2 };
static const u32 REC_CAP[3] = {256, 256, 170};   // records in a block
static const u32 REC_SIZE[3] = {16, 16, 24};

struct Stream { char* cur = nullptr; char* end = nullptr; u32 addr = 0; };
// One thread's blocks of one kind, grouped by partition; last[p] counts the records of the final block.
struct Lists { vector<u32> off, blocks, last; };

struct alignas(64) Ctx {
    vector<char> tail;
    Alloc alloc;
    vector<Stream> st[3];
    char* wc[2] = {nullptr, nullptr};  // one 64-byte line per token stream and per record stream
    vector<u64> bl[3];  // partition << 32 | block address, in the order the blocks were taken
    Lists ls[3];
    vector<pair<u64, u64>> links;
    // Event IDs that do not pack: where the bytes are (length << 47 | pointer, or bit 63 over an
    // index into `huge`). A token names its entry by thread and index.
    vector<u64> side;
    vector<pair<const char*, size_t>> huge;
    // dedup
    vector<u64> tab;
    // person stage
    vector<u64> hk;          // person -> local id: open addressing, parallel arrays
    vector<u32> hv;
    vector<u64> persons;     // by local id
    vector<u32> cnt, ccnt, lid, clid;
    vector<u64> w, sorted, tmp;
    vector<CItem> ck, cks;
    vector<u32> seen;
    u32 stamp = 0;
    vector<u32> wcnt, wpos, active;  // clicks per campaign in the 7-day window; the campaigns present
    vector<u64> catsum;
    vector<u32> touched;
    vector<AggHot> hot;
    vector<AggCold> cold;
};

static void grow(Ctx& me, int kind, u32 part) {
    u32 addr;
    char* b = me.alloc.block(addr);
    me.bl[kind].push_back((u64)part << 32 | addr);
    Stream& s = me.st[kind][part];
    s.cur = b;
    s.end = b + (size_t)REC_CAP[kind] * REC_SIZE[kind];
    s.addr = addr;
}
// A 16-byte record for stream `part`. With write combining the record lands in the stream's
// own line, and a full line goes to its block in one non-temporal store: no read of the
// destination, and a few thousand streams touch only a few hundred kilobytes.
static inline __attribute__((always_inline)) void put16(Ctx& me, int kind, u32 part, u64 a, u64 b) {
    Stream& s = me.st[kind][part];
    if (__builtin_expect(s.cur == s.end, 0)) grow(me, kind, part);
    if (USE_WC) {
        const size_t o = (uintptr_t)s.cur & 63;
        u64* line = (u64*)(me.wc[kind] + (size_t)part * 64 + o);
        line[0] = a;
        line[1] = b;
        s.cur += 16;
        if (o == 48) {
            const __m256i* src = (const __m256i*)(me.wc[kind] + (size_t)part * 64);
            _mm256_stream_si256((__m256i*)(s.cur - 64), _mm256_load_si256(src));
            _mm256_stream_si256((__m256i*)(s.cur - 32), _mm256_load_si256(src + 1));
        }
    } else {
        u64* r = (u64*)s.cur;
        r[0] = a;
        r[1] = b;
        s.cur += 16;
    }
}
static inline u32 put_rec(Ctx& me, u32 part, u64 person, u64 w) {
    put16(me, K_REC, part, person, w);
    return me.st[K_REC][part].addr++;
}
static inline void put_tok(Ctx& me, u32 part, u64 ev, u32 ing, u32 addr) {
    put16(me, K_TOK, part, ev, (u64)addr << 32 | ing);
}
// Moves what is left in the write-combining lines to the blocks.
static void flush_wc(Ctx& me, int kind) {
    if (!USE_WC) return;
    for (size_t part = 0; part < me.st[kind].size(); part++) {
        const Stream& s = me.st[kind][part];
        const size_t o = (uintptr_t)s.cur & 63;
        if (o) memcpy(s.cur - o, me.wc[kind] + part * 64, o);
    }
    _mm_sfence();
}
static inline void put_clk(Ctx& me, u32 part, const Clk& c) {
    Stream& s = me.st[K_CLK][part];
    if (__builtin_expect(s.cur == s.end, 0)) grow(me, K_CLK, part);
    *(Clk*)s.cur = c;
    s.cur += sizeof(Clk);
}

static void finish_lists(Ctx& me, int kind, u32 nparts) {
    Lists& L = me.ls[kind];
    L.off.assign(nparts + 1, 0);
    L.last.assign(nparts, 0);
    for (u64 e : me.bl[kind]) L.off[(e >> 32) + 1]++;
    for (u32 p = 0; p < nparts; p++) L.off[p + 1] += L.off[p];
    L.blocks.resize(me.bl[kind].size());
    vector<u32> pos(L.off.begin(), L.off.end() - 1);
    for (u64 e : me.bl[kind]) L.blocks[pos[e >> 32]++] = (u32)e;
    for (u32 p = 0; p < nparts; p++) {
        const Stream& s = me.st[kind][p];
        if (s.cur) L.last[p] = REC_CAP[kind] - (u32)((s.end - s.cur) / REC_SIZE[kind]);
    }
}

// Calls f(block, count) for every block of partition p, over all threads.
template <class T, class F>
static inline void each_block(vector<Ctx>& ctx, int kind, u32 p, F&& f) {
    for (auto& c : ctx) {
        const Lists& L = c.ls[kind];
        for (u32 j = L.off[p], e = L.off[p + 1]; j < e; j++) {
            if (j + 1 < e) {
                const char* nx = addr_ptr(L.blocks[j + 1]);
                __builtin_prefetch(nx);
                __builtin_prefetch(nx + 64);
                __builtin_prefetch(nx + 128);
            }
            f((T*)addr_ptr(L.blocks[j]), j + 1 == e ? L.last[p] : REC_CAP[kind]);
        }
    }
}

static inline void insertion_u64(u64* a, size_t n) {
    for (size_t i = 1; i < n; i++) {
        const u64 x = a[i];
        size_t j = i;
        for (; j && a[j - 1] > x; j--) a[j] = a[j - 1];
        a[j] = x;
    }
}

// Sorts events (ts in the high half) by ts. The elements agree on every ts bit from `hi` up.
// One counting pass on the next 8 bits leaves runs that are mostly a session long.
static void sort_ts(u64* a, size_t n, u64* tmp, int hi) {
    if (n <= 24) { insertion_u64(a, n); return; }
    if (hi <= 0) return;  // one timestamp: any order will do
    const int d = hi < 8 ? hi : 8, shift = 32 + hi - d;
    const u64 mask = (1u << d) - 1;
    u32 cnt[257] = {0};
    for (size_t i = 0; i < n; i++) cnt[((a[i] >> shift) & mask) + 1]++;
    for (int b = 0; b < 256; b++) cnt[b + 1] += cnt[b];
    u32 pos[256];
    memcpy(pos, cnt, sizeof pos);
    for (size_t i = 0; i < n; i++) tmp[pos[(a[i] >> shift) & mask]++] = a[i];
    memcpy(a, tmp, n * sizeof(u64));
    for (int b = 0; b < 256; b++) {
        const size_t k = cnt[b + 1] - cnt[b];
        if (k > 1) sort_ts(a + cnt[b], k, tmp + cnt[b], hi - d);
    }
}

// Clicks of one person by ts, and by click_id where the ts ties: the first of a tie gets the credit.
static inline bool click_less(const CItem& a, const CItem& b) {
    const u32 ta = (u32)(a.tc >> 32), tb = (u32)(b.tc >> 32);
    if (ta != tb) return ta < tb;
    return a.id != b.id && key_less(a.id, b.id);
}
static void sort_clicks(CItem* a, size_t n) {
    if (n <= 16) {
        for (size_t i = 1; i < n; i++) {
            const CItem x = a[i];
            size_t j = i;
            for (; j && click_less(x, a[j - 1]); j--) a[j] = a[j - 1];
            a[j] = x;
        }
    } else {
        sort(a, a + n, click_less);
    }
}

// user_daily rows of one thread, written out a row group at a time.
struct UserRows {
    PqFile f;
    bool opened = false;
    string path;
    string pg[6], cats;
    vector<u8> present;
    u32 rows = 0;
    static const u32 GROUP = 16384;
    void open_file() {
        f.create(path, {{"user_id", C_STR}, {"day", C_DATE}, {"sessions", C_I32}, {"events", C_I32},
                        {"revenue_cents", C_I64}, {"top_category", C_STR_OPT}});
        opened = true;
    }
    inline void add(u64 person, int32_t day, u32 sessions, u32 events, u64 revenue, int cat) {
        char tmp[16];
        put_str(pg[0], key_str(person, tmp));
        put32(pg[1], (u32)day);
        put32(pg[2], sessions);
        put32(pg[3], events);
        put64(pg[4], revenue);
        present.push_back(cat >= 0);
        if (cat >= 0) put_str(cats, cat_names[cat]);
        if (++rows == GROUP) flush();
    }
    void flush() {
        if (!rows) return;
        if (!opened) open_file();
        pg[5] = def_levels(present);
        pg[5] += cats;
        f.group(rows, pg);
        for (auto& x : pg) x.clear();
        cats.clear();
        present.clear();
        rows = 0;
    }
    void finish(bool always) {
        flush();
        if (!opened && always) open_file();
        if (opened) f.finish();
    }
};

static string read_file(const string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) die("cannot open " + path);
    string s;
    char buf[65536];
    for (;;) {
        ssize_t r = read(fd, buf, sizeof buf);
        if (r < 0) { if (errno == EINTR) continue; die("read failed: " + path); }
        if (r == 0) break;
        s.append(buf, (size_t)r);
    }
    close(fd);
    return s;
}

static vector<MFile> list_csv(const string& dir) {
    vector<MFile> out;
    DIR* d = opendir(dir.c_str());
    if (!d) return out;
    while (dirent* e = readdir(d)) {
        string name = e->d_name;
        if (name.size() < 4 || name[0] == '.' || name.compare(name.size() - 4, 4, ".csv") != 0) continue;
        string path = dir + "/" + name;
        struct stat st;
        if (stat(path.c_str(), &st) != 0 || !S_ISREG(st.st_mode) || st.st_size == 0) continue;
        out.push_back(map_file(path));
    }
    closedir(d);
    return out;
}

struct Work { int kind; const MFile* f; u64 a, b; };
static void add_chunks(vector<Work>& w, const vector<MFile>& files, int kind, u64 chunk) {
    for (auto& f : files)
        for (u64 a = 0; a < f.size; a += chunk) w.push_back({kind, &f, a, a + chunk});
}

static string json_value(const string& j, const char* key) {
    size_t p = j.find(string("\"") + key + "\"");
    if (p == string::npos) die(string("manifest lacks ") + key);
    p = j.find(':', p);
    size_t a = j.find('"', p);
    size_t b = j.find('"', a + 1);
    if (p == string::npos || a == string::npos || b == string::npos) die("bad manifest");
    return j.substr(a + 1, b - a - 1);
}

static void init_dates() {
    const i64 d0 = floor_div(BASE, DAY_MS) - 1, d1 = floor_div(WE - 1, DAY_MS) + 7;
    bool ok = true;
    for (i64 dd = d0; dd <= d1 && ok; dd++) {
        i64 y;
        unsigned m, d;
        civil_from_days(dd, y, m, d);
        if (y < 0 || y > 9999) continue;
        char t[32];
        snprintf(t, sizeof t, "%04d-%02u-%02u", (int)y, m, d);
        u64 a;
        uint16_t b;
        memcpy(&a, t, 8);
        memcpy(&b, t + 8, 2);
        DateEnt& e = DATES[date_slot(t)];
        if (e.b) ok = false;  // two days in one slot: a window too long for this table
        e.a = a;
        e.b = (u32)b | 0x10000;
        e.day = (int32_t)dd;
    }
    DATES_OK = ok;
}

// The number written in p[skip..n): 1 to 7 digits, no leading zero.
static inline bool canon_num(const char* p, u32 n, u32 skip, u32& v) {
    const u32 nd = n - skip;
    if (nd < 1 || nd > 7 || (p[skip] == '0' && nd > 1)) return false;
    u32 x = 0;
    for (u32 i = skip; i < n; i++) {
        const unsigned d = (unsigned)((u8)p[i] - '0');
        if (d > 9) return false;
        x = x * 10 + d;
    }
    v = x;
    return true;
}
static inline u32 product_cat(const char* p, u32 n) {
    u32 v;
    if (n >= 2 && p[0] == 'p' && canon_num(p, n, 1, v) && v < prod_direct.size() && prod_direct[v] != 0xFF)
        return prod_direct[v];
    const u64* c = products.find(pack(p, n));
    return c ? (u32)*c : (u32)CAT_UNKNOWN;
}
// The campaign's index, or -1.
static inline int campaign_of(const char* p, u32 n) {
    u32 v;
    if (n >= 3 && p[0] == 'c' && p[1] == '_' && canon_num(p, n, 2, v) && v < camp_direct.size() &&
        camp_direct[v] != 0xFFFF)
        return (int)camp_direct[v];
    const u64* c = campaigns.find(pack(p, n));
    return c ? (int)*c : -1;
}

static void load_products(const string& path, vector<char>& tail) {
    MFile pf = map_file(path);
    unordered_map<string, int> cat_idx;
    vector<pair<u64, u64>> rows;
    vector<pair<u32, u32>> direct;
    u32 top = 0;
    auto cat_of = [&](const string& name) {
        auto it = cat_idx.find(name);
        if (it != cat_idx.end()) return it->second;
        int id = (int)cat_names.size();
        cat_names.push_back(name);
        cat_idx.emplace(name, id);
        return id;
    };
    if (pf.size)
        process_chunk<2>(pf, 0, pf.size, tail, [&](const char** f, const u32* l) {
            if (!l[0]) return;
            const u32 cat = (u32)cat_of(string(f[1], l[1]));
            u32 v;
            if (l[0] >= 2 && f[0][0] == 'p' && canon_num(f[0], l[0], 1, v) && v < PROD_DIRECT_MAX) {
                direct.push_back({v, cat});
                if (v >= top) top = v + 1;
            } else {
                rows.push_back({pack(f[0], l[0]), (u64)cat});
            }
        });
    CAT_UNKNOWN = cat_of("unknown");
    if (cat_names.size() > 255) die("too many categories for this build");
    products.init(rows.size());
    for (auto& r : rows) products.at(r.first, r.second);
    prod_direct.assign(top, 0xFF);
    for (auto& r : direct)
        if (prod_direct[r.first] == 0xFF) prod_direct[r.first] = (u8)r.second;
    vector<u32> order(cat_names.size());
    for (u32 i = 0; i < order.size(); i++) order[i] = i;
    sort(order.begin(), order.end(), [&](u32 a, u32 b) { return cat_names[a] < cat_names[b]; });
    cat_rank.resize(order.size());
    for (u32 i = 0; i < order.size(); i++) cat_rank[order[i]] = i;
}

static void load_campaigns(const string& path, vector<char>& tail) {
    MFile cf = map_file(path);
    vector<pair<u64, u64>> keys;
    vector<pair<u32, u32>> direct;
    u32 top = 0;
    if (cf.size)
        process_chunk<2>(cf, 0, cf.size, tail, [&](const char** f, const u32* l) {
            if (!l[0]) return;
            const u32 idx = (u32)camp_ids.size();
            u32 v;
            if (idx < 0xFFFF && l[0] >= 3 && f[0][0] == 'c' && f[0][1] == '_' && canon_num(f[0], l[0], 2, v) &&
                v < CAMP_DIRECT_MAX) {
                direct.push_back({v, idx});
                if (v >= top) top = v + 1;
            } else
                keys.push_back({pack(f[0], l[0]), (u64)idx});
            camp_ids.emplace_back(f[0], l[0]);
            camp_channels.emplace_back(f[1], l[1]);
        });
    NCAMP = (int)camp_ids.size();
    campaigns.init(keys.size());
    for (auto& k : keys) campaigns.at(k.first, k.second);
    camp_direct.assign(top, 0xFFFF);
    for (auto& r : direct)
        if (camp_direct[r.first] == 0xFFFF) camp_direct[r.first] = (uint16_t)r.second;
    camp_ids.push_back("organic");
    camp_channels.push_back("organic");
}

// ---------------------------------------------------------------------------
// Linked IDs: a lock-free hash table of every linked ID, with a union-find laid over its slots.
// While it is built, v is the parent slot plus one, 0 at a root. A root always holds the
// byte-wise smallest ID of its set, because a union hangs the root with the larger ID under the
// other. A closing pass turns every v into the person's name, the root's ID. A slot number is
// below 2^32 and an ID is not, so a reader of v can tell which it holds.
// ---------------------------------------------------------------------------
struct LEnt { atomic<u64> key; atomic<u64> v; };
static LEnt* LT = nullptr;
static u64 LN = 0, LNB = 0;  // slots, and cache lines of 4 slots

static inline u64 lhash(u64 k) {
    k ^= k >> 32;
    k *= 0x9E3779B97F4A7C15ull;
    k ^= k >> 29;
    return k;
}
// A probe starts at the first slot of a cache line, so it rarely leaves the line.
static inline size_t lhome(u64 k) { return (size_t)(((unsigned __int128)lhash(k) * LNB) >> 64) << 2; }

static inline size_t link_slot(u64 k, bool& created) {
    size_t i = lhome(k);
    for (;;) {
        u64 cur = LT[i].key.load(memory_order_relaxed);
        if (cur == k) { created = false; return i; }
        if (cur == 0) {
            if (LT[i].key.compare_exchange_strong(cur, k, memory_order_relaxed)) { created = true; return i; }
            if (cur == k) { created = false; return i; }
        }
        if (++i == LN) i = 0;
    }
}
static inline size_t link_root(size_t x) {
    for (;;) {
        const u64 p = LT[x].v.load(memory_order_relaxed);
        if (!p) return x;
        const u64 gp = LT[p - 1].v.load(memory_order_relaxed);
        if (gp) {  // halve the path; losing this race to another thread is harmless
            u64 expect = p;
            LT[x].v.compare_exchange_weak(expect, gp, memory_order_relaxed);
        }
        x = p - 1;
    }
}
static inline void link_unite(size_t a, size_t b) {
    for (;;) {
        a = link_root(a);
        b = link_root(b);
        if (a == b) return;
        if (key_less(LT[b].key.load(memory_order_relaxed), LT[a].key.load(memory_order_relaxed))) swap(a, b);
        u64 expect = 0;  // b has the larger ID; it stops being a root, unless someone got there first
        if (LT[b].v.compare_exchange_strong(expect, a + 1, memory_order_relaxed)) return;
    }
}

// user id -> person name, from the first slot of the ID's home line. Only after the table is
// finished. The four IDs of a line are compared at once and the answer picked without a branch
// on hit or miss, which no predictor could learn. An ID that is not in its line while the line
// has a free slot is in no line.
static inline __attribute__((always_inline)) u64 resolve_at(size_t i, u64 user) {
    const u64* T = (const u64*)LT;
    const __m256i u = _mm256_set1_epi64x((long long)user), z = _mm256_setzero_si256();
    for (;;) {
        const __m256i a = _mm256_load_si256((const __m256i*)(T + 2 * i));      // id, name, id, name
        const __m256i b = _mm256_load_si256((const __m256i*)(T + 2 * i + 4));
        const unsigned hit = ((unsigned)_mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(a, u))) & 5) |
                             ((unsigned)_mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(b, u))) & 5) << 4;
        const unsigned empty = ((unsigned)_mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(a, z))) & 5) |
                               ((unsigned)_mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(b, z))) & 5) << 4;
        if (__builtin_expect((hit | empty) != 0, 1)) {
            const u64 name = T[2 * i + (size_t)__builtin_ctz(hit | 0x100) + 1];  // a miss reads past the line; the table is padded
            return hit ? name : user;
        }
        i += 4;
        if (i == LN) i = 0;
    }
}

// ---------------------------------------------------------------------------
int main() {
    T0 = now_s();
    init_codes();
    init_simd_tables();
    auto env = [](const char* k, const char* d) { const char* v = getenv(k); return string(v && *v ? v : d); };
    string DATA = env("DATA_DIR", "/data"), OUT = env("OUTPUT_DIR", "/output");
    NT = atoi(env("NCPU", "0").c_str());
    if (NT <= 0) NT = (int)thread::hardware_concurrency();
    if (NT <= 0) NT = 1;
    if (NT > 256) NT = 256;
    u64 CHUNK = strtoull(env("SS_CHUNK", "1048576").c_str(), nullptr, 10);
    if (CHUNK < 1) CHUNK = 1;
    const int PARTS = atoi(env("SS_PARTS", "0").c_str());
    USE_THP = !getenv("SS_NOTHP");
    USE_WC = !getenv("SS_NOWC");
    USE_MMAP = !getenv("SS_NOMMAP");

    // manifest
    {
        string j = read_file(DATA + "/manifest.json");
        string a = json_value(j, "window_start"), b = json_value(j, "window_end");
        if (!parse_ts(a.data(), a.size(), WS) || !parse_ts(b.data(), b.size(), WE)) die("bad manifest window");
        BASE = WS - WEEK_MS;
        if (WE < WS || WE - BASE >= (i64)TS_INVALID) die("window too long for this build");
        DAY_BASE = floor_div(BASE, DAY_MS);
        DAY_OFF = BASE - DAY_BASE * DAY_MS;
        NDAYS = (int)((WE - BASE + DAY_OFF) / DAY_MS + 1);
        ING_BASE = floor_div(BASE, 1000) - (1ll << 30);
        if (!getenv("SS_NOFAST")) init_dates();
    }

    vector<MFile> ev_files = list_csv(DATA + "/events"), ck_files = list_csv(DATA + "/clicks"),
                  ln_files = list_csv(DATA + "/links");
    u64 ev_bytes = 0;
    for (auto& f : ev_files) ev_bytes += f.size;
    // Person partitions: about 64K events each. Token partitions: about 21K, so a dedup table stays in cache.
    int P = 16, TB = 0;
    if (PARTS > 0) { P = 1; while (P < PARTS) P <<= 1; }
    else { while (P < 4096 && (u64)P * 65536 * 80 < ev_bytes) P <<= 1; }
    const u64 PMASK = (u64)P - 1;
    {
        const int want = atoi(env("SS_TP", "0").c_str());
        const u64 target = want > 0 ? (u64)want : PARTS > 0 ? (u64)PARTS : ev_bytes / 75 / 21000;
        while (TB < 12 && (3ull << TB) < target * 2) TB++;  // the power of two nearest the target
    }
    const u32 TP = 1u << TB;
    auto tpart = [TB](u64 ev) -> u32 {
        if (!TB) return 0;
        const u32 a = (u32)((ev * 0x9E3779B97F4A7C15ull) >> (64 - TB)), b = (u32)((ev << 1) >> (64 - TB));
        return (ev & FB) ? b : a;
    };
    {
        u64 all = ev_bytes;
        for (auto* v : {&ck_files, &ln_files})
            for (auto& f : *v) all += f.size;
        const u64 want = strtoull(env("SS_INTERN", "0").c_str(), nullptr, 10);
        intern.init(want ? (size_t)want : (size_t)max<u64>(4096, all / 2048));
    }

    pool.start(NT);
    vector<Ctx> ctx(NT);
    for (auto& c : ctx) {
        c.st[K_TOK].resize(TP);
        c.st[K_REC].resize(P);
        c.st[K_CLK].resize(P);
    }
    lap("setup");

    // ---- phase 1: products, campaigns, links
    {
        vector<Work> work;
        work.push_back({3, nullptr, 0, 0});
        work.push_back({4, nullptr, 0, 0});
        add_chunks(work, ln_files, 0, CHUNK);
        atomic<size_t> next{0};
        pool.run([&](int t) {
            Ctx& me = ctx[t];
            for (size_t i; (i = next.fetch_add(1)) < work.size();) {
                Work& w = work[i];
                if (w.kind == 3) load_products(DATA + "/products.csv", me.tail);
                else if (w.kind == 4) load_campaigns(DATA + "/campaigns.csv", me.tail);
                else
                    process_chunk<2>(*w.f, w.a, w.b, me.tail, [&](const char** f, const u32* l) {
                        if (!l[0] || !l[1]) return;
                        u64 a, b;
                        const unsigned ok = pack2(f[0], l[0], f[1], l[1], a, b);
                        if (!(ok & 1)) a = pack(f[0], l[0]);
                        if (!(ok & 2)) b = pack(f[1], l[1]);
                        if (a != b) me.links.push_back({a, b});
                    });
            }
        });
        size_t pairs = 0;
        for (auto& c : ctx) pairs += c.links.size();
        if (pairs) {
            LNB = pairs + 16;       // at most 2 IDs a pair, so the table is at most half full
            LN = LNB * 4;
            if (LN >= 0xFFFFFFFFull) die("too many links for this build");
            LT = (LEnt*)big_alloc(LN * sizeof(LEnt) + 128);
            pool.run([&](int t) {   // insert and unite
                const auto& pr = ctx[t].links;
                const size_t n = pr.size();
                size_t sa[16], sb[16];
                for (size_t i = 0; i < n; i += 16) {
                    const size_t e = min(n, i + 16);
                    for (size_t j = i; j < e; j++) {
                        __builtin_prefetch(&LT[lhome(pr[j].first)], 1);
                        __builtin_prefetch(&LT[lhome(pr[j].second)], 1);
                    }
                    for (size_t j = i; j < e; j++) {  // take the slots, and ask for the parents' lines
                        bool created;
                        const size_t a = link_slot(pr[j].first, created), b = link_slot(pr[j].second, created);
                        sa[j - i] = a;
                        sb[j - i] = b;
                        const u64 pa = LT[a].v.load(memory_order_relaxed), pb = LT[b].v.load(memory_order_relaxed);
                        if (pa) __builtin_prefetch(&LT[pa - 1], 1);
                        if (pb) __builtin_prefetch(&LT[pb - 1], 1);
                    }
                    for (size_t j = i; j < e; j++) link_unite(sa[j - i], sb[j - i]);
                }
            });
            // Names. Whoever walks up from a slot stops at a root, whose name is its own ID, or at
            // a slot that already holds a name: it is the same one.
            atomic<size_t> at{0};
            pool.run([&](int) {
                const size_t step = 1 << 15;
                for (size_t lo; (lo = at.fetch_add(step)) < LN;) {
                    const size_t hi = min<size_t>(LN, lo + step);
                    for (size_t i = lo; i < hi; i++) {
                        if (i + 16 < hi) {
                            const u64 ahead = LT[i + 16].v.load(memory_order_relaxed);
                            if (ahead && ahead < (1ull << 32)) __builtin_prefetch(&LT[ahead - 1]);
                        }
                        const u64 key = LT[i].key.load(memory_order_relaxed);
                        if (!key) continue;
                        u64 name = key;
                        for (size_t x = i;;) {
                            const u64 v = LT[x].v.load(memory_order_relaxed);
                            if (!v) { name = LT[x].key.load(memory_order_relaxed); break; }
                            if (v >= (1ull << 32)) { name = v; break; }
                            x = v - 1;
                        }
                        LT[i].v.store(name, memory_order_relaxed);
                    }
                }
            });
        }
    }
    lap("links");

    // ---- phase 2: events and clicks
    {
        vector<Work> work;
        add_chunks(work, ev_files, 1, CHUNK);
        add_chunks(work, ck_files, 2, CHUNK);
        atomic<size_t> next{0};
        const i64 ws = WS, we = WE, base = BASE;
        struct PendEv { u64 user, ev; u32 ts, pc, ing, home; };
        struct PendCk { u64 user, id; u32 ts, camp, home; };
        const bool linked = LN != 0;
        pool.run([&](int t) {
            Ctx& me = ctx[t];
            // A record needs its person, a lookup that misses the cache: rows wait in a ring
            // until the line asked for 16 rows ago has arrived.
            PendEv ering[32];
            PendCk cring[32];
            unsigned eh = 0, et = 0, ch = 0, ct = 0;
            if (USE_WC) {
                me.wc[K_TOK] = (char*)aligned_alloc(64, (size_t)TP * 64);
                me.wc[K_REC] = (char*)aligned_alloc(64, (size_t)P * 64);
                if (!me.wc[K_TOK] || !me.wc[K_REC]) die("out of memory");
            }
            auto emit_ev = [&](const PendEv& e) __attribute__((always_inline)) {
                const u64 person = linked ? resolve_at((size_t)e.home << 2, e.user) : e.user;
                const u32 addr = put_rec(me, (u32)(mix(person) >> 32 & PMASK), person, (u64)e.ts << 32 | e.pc);
                put_tok(me, tpart(e.ev), e.ev, e.ing, addr);
            };
            auto emit_ck = [&](const PendCk& c) __attribute__((always_inline)) {
                const u64 person = linked ? resolve_at((size_t)c.home << 2, c.user) : c.user;
                put_clk(me, (u32)(mix(person) >> 32 & PMASK), Clk{person, c.id, c.ts, c.camp});
            };
            // Bytes that the unit of work will overwrite (the padded copy of a file's end) are
            // copied; everything else is in an input that stays mapped.
            auto stable = [&](const char* p, size_t n) -> const char* {
                return (uintptr_t)p - (uintptr_t)me.tail.data() < me.tail.size() ? arena_copy(p, n) : p;
            };
            auto long_ev = [&](const char* p, size_t n) __attribute__((noinline)) -> u64 {
                const u64 h = hash_bytes(p, n);
                const char* q = stable(p, n);
                const size_t idx = me.side.size();
                if (idx >> 28) die("too many long event ids for this build");
                if (n < (1u << 16) && !((u64)(uintptr_t)q >> 47)) {  // bit 63 stays clear
                    me.side.push_back((u64)n << 47 | (u64)(uintptr_t)q);
                } else {
                    me.side.push_back(FB | (u64)me.huge.size());
                    me.huge.push_back({q, n});
                }
                return FB | (h >> 37 << 36) | (u64)t << 28 | (u64)idx;
            };
            auto ref_key = [&](const char* p, size_t n) __attribute__((noinline)) -> u64 {
                if (n >= REF_MAXLEN) return intern.get(p, n);
                const char* q = stable(p, n);
                if ((u64)(uintptr_t)q >> 47) return intern.get(p, n);
                return FB | TAG_REF | (u64)n << 47 | (u64)(uintptr_t)q;
            };
            auto on_event = [&](const char** f, const u32* l) __attribute__((always_inline)) {
                if (!l[0]) return;
                u64 ev, user;
                const unsigned packed = pack2(f[0], l[0], f[1], l[1], ev, user);
                if (__builtin_expect(!(packed & 1), 0)) ev = long_ev(f[0], l[0]);
                i64 ing = 0, ts = 0;
                if (!parse_ts_any(f[3], l[3], ing)) ing = 0;
                const i64 isec = floor_div(ing, 1000) - ING_BASE;
                const u32 ing32 = isec < 0 ? 0u : isec > 0xFFFFFFFFll ? 0xFFFFFFFFu : (u32)isec;
                u32 pc = 0;
                bool valid = false;
                do {
                    if (!l[1] || !l[5]) break;
                    if (!parse_ts_win(f[2], l[2], ts) || ts < ws || ts >= we) break;
                    if (ing - ts > DAY_MS) break;
                    const char* ty = f[4];
                    if (l[4] == 4) {
                        if (memcmp(ty, "view", 4) != 0 && memcmp(ty, "cart", 4) != 0) break;
                    } else if (l[4] == 8 && memcmp(ty, "purchase", 8) == 0) {
                        const char* p = f[6];
                        u32 n = l[6], i = 0;
                        if (!n) break;
                        bool ok = true;
                        for (u32 k = 0; k < n; k++)
                            if ((unsigned)((u8)p[k] - '0') > 9) { ok = false; break; }
                        if (!ok) break;
                        while (i < n && p[i] == '0') i++;
                        if (n - i > 8) break;
                        u32 v = 0;
                        for (; i < n; i++) v = v * 10 + (u32)(p[i] - '0');
                        if (v < 1 || v > 10000000) break;
                        pc = (v << 8) | product_cat(f[5], l[5]);
                    } else {
                        break;
                    }
                    valid = true;
                } while (0);
                if (!valid) { put_tok(me, tpart(ev), ev, ing32, NOADDR); return; }
                if (__builtin_expect(!(packed & 2), 0)) user = pack(f[1], l[1]);
                PendEv& e = ering[et++ & 31];
                e.user = user;
                e.ev = ev;
                e.ts = (u32)(ts - base);
                e.pc = pc;
                e.ing = ing32;
                if (linked) {
                    const size_t home = lhome(user);
                    e.home = (u32)(home >> 2);
                    __builtin_prefetch(&LT[home]);
                }
                if (et - eh > 16) emit_ev(ering[eh++ & 31]);
            };
            auto on_click = [&](const char** f, const u32* l) __attribute__((always_inline)) {
                if (!l[0] || !l[1] || !l[2]) return;
                i64 ts;
                if (!parse_ts_win(f[3], l[3], ts) || ts < base || ts >= we) return;
                const int camp = campaign_of(f[2], l[2]);
                if (camp < 0) return;
                u64 id, user;
                const unsigned packed = pack2(f[0], l[0], f[1], l[1], id, user);
                if (__builtin_expect(!(packed & 1), 0)) id = ref_key(f[0], l[0]);
                if (__builtin_expect(!(packed & 2), 0)) user = pack(f[1], l[1]);
                PendCk& k = cring[ct++ & 31];
                k.user = user;
                k.id = id;
                k.ts = (u32)(ts - base);
                k.camp = (u32)camp;
                if (linked) {
                    const size_t home = lhome(user);
                    k.home = (u32)(home >> 2);
                    __builtin_prefetch(&LT[home]);
                }
                if (ct - ch > 16) emit_ck(cring[ch++ & 31]);
            };
            for (size_t i; (i = next.fetch_add(1)) < work.size();) {
                Work& w = work[i];
                if (w.kind == 1) {
                    process_chunk<7>(*w.f, w.a, w.b, me.tail, on_event);
                    while (eh != et) emit_ev(ering[eh++ & 31]);
                } else {
                    process_chunk<4>(*w.f, w.a, w.b, me.tail, on_click);
                    while (ch != ct) emit_ck(cring[ch++ & 31]);
                }
            }
            flush_wc(me, K_TOK);
            flush_wc(me, K_REC);
            finish_lists(me, K_TOK, TP);
            finish_lists(me, K_REC, (u32)P);
            finish_lists(me, K_CLK, (u32)P);
        });
    }
    lap("events and clicks parsed");

    // The inputs are no longer needed. Unmapping 2 GB one page at a time is slow on the way out
    // of a process, where one thread does it; here the workers share it, at the start of phase 3.
    // The link files go now. Event files go once every thread is done with dedup, which reads
    // the long event IDs in them; click files once the person stage is done with the long click IDs.
    atomic<size_t> unmap_ln{0}, unmap_ev{0}, unmap_ck{0};
    auto unmap_some = [](const vector<MFile>& files, atomic<size_t>& at) {
        for (size_t i; (i = at.fetch_add(1)) < files.size();)
            if (files[i].is_map) munmap((void*)files[i].p, files[i].size);
    };

    // ---- phase 3: dedup. Among the copies of an event the smallest ingest_ts wins; every other
    // copy that made a record gets the record dropped. An invalid winner has no record at all.
    {
        atomic<u32> next{0};
        pool.run([&](int t) {
            Ctx& me = ctx[t];
            vector<const Tok*> bp;
            vector<u32> bn;
            auto drop = [](u32 addr) {
                if (addr != NOADDR) ((PEv*)addr_ptr(addr))->w = ~0ull;
            };
            unmap_some(ln_files, unmap_ln);
            auto id_bytes = [&](u64 ev) -> string_view {
                const Ctx& c = ctx[(ev >> 28) & 255];
                const u64 e = c.side[ev & 0xFFFFFFF];
                if (e & FB) return string_view(c.huge[e & ~FB].first, c.huge[e & ~FB].second);
                return string_view((const char*)(uintptr_t)(e & PTR47), (size_t)(e >> 47));
            };
            for (u32 p; (p = next.fetch_add(1)) < TP;) {
                bp.clear();
                bn.clear();
                size_t n = 0;
                each_block<Tok>(ctx, K_TOK, p, [&](const Tok* b, u32 k) { bp.push_back(b); bn.push_back(k); n += k; });
                if (n < 2) continue;
                size_t cap = 64;
                while (cap < n * 2) cap <<= 1;
                if (me.tab.size() < cap) me.tab.resize(cap);
                u64* tab = me.tab.data();
                memset(tab, 0, cap * sizeof(u64));
                const u64 m = cap - 1;
                // A slot is 32 hash bits over (block << 8 | index): the token is read only when
                // the hash bits match, which is nearly always a true duplicate.
                for (size_t o = 0; o < bp.size(); o++) {
                    const Tok* b = bp[o];
                    for (u32 i = 0, k = bn[o]; i < k; i++) {
                        const Tok& tk = b[i];
                        const u64 kev = ev_key(tk.ev);
                        const u64 h = mix(kev);
                        const u64 tag = (h >> 32) | 1;
                        const u64 self = tag << 32 | (u64)(o << 8 | i);
                        for (u64 s = h & m;; s = (s + 1) & m) {
                            const u64 e = tab[s];
                            if (!e) { tab[s] = self; break; }
                            if ((e >> 32) != tag) continue;
                            const Tok& ot = bp[(u32)e >> 8][(u32)e & 255];
                            if (ev_key(ot.ev) != kev) continue;
                            if ((kev & FB) && id_bytes(ot.ev) != id_bytes(tk.ev)) continue;
                            if (tk.ing < ot.ing) { drop(ot.addr); tab[s] = self; }
                            else drop(tk.addr);
                            break;
                        }
                    }
                }
            }
        });
    }
    lap("deduped");

    // ---- phase 4: per person: gather, sort by time, sessionize, credit, write rows
    mkdir(OUT.c_str(), 0755);
    const string ud_dir = OUT + "/user_daily", cd_dir = OUT + "/campaign_daily";
    mkdir(ud_dir.c_str(), 0755);
    mkdir(cd_dir.c_str(), 0755);
    atomic<u64> total_rows{0};
    const size_t ncell = (size_t)(NCAMP + 1) * NDAYS;
    {
        // Largest partitions first: a whale's partition should not be the last one started.
        vector<pair<u64, u32>> order(P);
        for (int p = 0; p < P; p++) {
            u64 n = 0;
            for (auto& c : ctx) {
                const Lists& L = c.ls[K_REC];
                if (L.off[p + 1] > L.off[p]) n += (u64)(L.off[p + 1] - L.off[p] - 1) * REC_CAP[K_REC] + L.last[p];
            }
            order[p] = {n, (u32)p};
        }
        sort(order.begin(), order.end(), [](const auto& a, const auto& b) { return a.first > b.first; });
        int top_hi = 0;  // bits a relative ts can use
        while (top_hi < 32 && (1ll << top_hi) < WE - BASE) top_hi++;
        atomic<int> next{0};
        const int nd = NDAYS;
        pool.run([&](int t) {
            Ctx& me = ctx[t];
            unmap_some(ev_files, unmap_ev);  // every thread is past dedup
            me.seen.assign(NCAMP + 1, 0);
            me.wcnt.assign(NCAMP + 1, 0);
            me.wpos.assign(NCAMP + 1, 0);
            me.catsum.assign(cat_names.size(), 0);
            me.hot.assign(ncell, AggHot{0, 0});
            me.cold.assign(ncell, AggCold{0, 0});
            UserRows out;
            char name[64];
            snprintf(name, sizeof name, "/part-%03d.parquet", t);
            out.path = ud_dir + name;
            u64 nrows = 0;
            for (int oi; (oi = next.fetch_add(1)) < P;) {
                const u32 p = order[oi].second;
                const size_t n = order[oi].first;
                if (!n) continue;
                // local ids: person -> 0, 1, 2, ... in order of first sight
                size_t cap = 256;
                while (cap < 4096 && cap < n) cap <<= 1;
                int hshift = 64 - __builtin_ctzll(cap);
                if (me.hk.size() < cap) { me.hk.resize(cap); me.hv.resize(cap); }
                memset(me.hk.data(), 0, cap * sizeof(u64));
                me.persons.clear();
                me.cnt.clear();
                if (me.w.size() < n) { me.w.resize(n); me.lid.resize(n); me.sorted.resize(n); me.tmp.resize(n); }
                u64* hk = me.hk.data();
                u32* hv = me.hv.data();
                auto enlarge = [&]() {
                    cap <<= 1;
                    hshift--;
                    if (me.hk.size() < cap) { me.hk.resize(cap); me.hv.resize(cap); }
                    hk = me.hk.data();
                    hv = me.hv.data();
                    memset(hk, 0, cap * sizeof(u64));
                    for (u32 id = 0; id < me.persons.size(); id++) {
                        size_t i = (size_t)((me.persons[id] * 0x9E3779B97F4A7C15ull) >> hshift);
                        while (hk[i]) i = (i + 1) & (cap - 1);
                        hk[i] = me.persons[id];
                        hv[i] = id;
                    }
                };
                size_t m = 0;
                each_block<PEv>(ctx, K_REC, p, [&](const PEv* b, u32 k) {
                    for (u32 x = 0; x < k; x++) {
                        if (!alive(b[x])) continue;
                        const u64 person = b[x].person;
                        size_t i = (size_t)((person * 0x9E3779B97F4A7C15ull) >> hshift);
                        u32 id;
                        for (;;) {
                            const u64 key = hk[i];
                            if (key == person) { id = hv[i]; break; }
                            if (!key) {
                                id = (u32)me.persons.size();
                                hk[i] = person;
                                hv[i] = id;
                                me.persons.push_back(person);
                                me.cnt.push_back(0);
                                if (me.persons.size() * 2 > cap) enlarge();
                                break;
                            }
                            i = (i + 1) & (cap - 1);
                        }
                        me.cnt[id]++;
                        me.w[m] = b[x].w;
                        me.lid[m] = id;
                        m++;
                    }
                });
                if (!m) continue;
                const u32 np = (u32)me.persons.size();
                // events grouped by person: cnt becomes each person's start
                {
                    u32 run = 0;
                    for (u32 id = 0; id < np; id++) { const u32 c = me.cnt[id]; me.cnt[id] = run; run += c; }
                    me.cnt.push_back(run);
                }
                {
                    // scatter with a moving cursor per person, then restore the starts
                    u32* start = me.cnt.data();
                    const u64* w = me.w.data();
                    const u32* lid = me.lid.data();
                    u64* sorted = me.sorted.data();
                    for (size_t i = 0; i < m; i++) sorted[start[lid[i]]++] = w[i];
                    for (u32 id = np; id > 0; id--) start[id] = start[id - 1];
                    start[0] = 0;
                }
                // clicks of the people who have events here
                size_t nck = 0;
                {
                    size_t total = 0;
                    each_block<Clk>(ctx, K_CLK, p, [&](const Clk*, u32 k) { total += k; });
                    if (me.ck.size() < total) { me.ck.resize(total); me.cks.resize(total); me.clid.resize(total); }
                    me.ccnt.assign(np + 1, 0);
                    each_block<Clk>(ctx, K_CLK, p, [&](const Clk* b, u32 k) {
                        for (u32 x = 0; x < k; x++) {
                            const u64 person = b[x].user;
                            size_t i = (size_t)((person * 0x9E3779B97F4A7C15ull) >> hshift);
                            for (;;) {
                                const u64 key = hk[i];
                                if (key == person) {
                                    const u32 id = hv[i];
                                    me.ccnt[id]++;
                                    me.ck[nck] = CItem{(u64)b[x].ts << 32 | b[x].camp, b[x].id};
                                    me.clid[nck] = id;
                                    nck++;
                                    break;
                                }
                                if (!key) break;
                                i = (i + 1) & (cap - 1);
                            }
                        }
                    });
                    u32 run = 0;
                    for (u32 id = 0; id < np; id++) { const u32 c = me.ccnt[id]; me.ccnt[id] = run; run += c; }
                    me.ccnt[np] = run;
                    u32* cstart = me.ccnt.data();
                    for (size_t i = 0; i < nck; i++) me.cks[cstart[me.clid[i]]++] = me.ck[i];
                    for (u32 id = np; id > 0; id--) cstart[id] = cstart[id - 1];
                    cstart[0] = 0;
                }

                for (u32 id = 0; id < np; id++) {
                    const u64 person = me.persons[id];
                    u64* ev = me.sorted.data() + me.cnt[id];
                    const size_t ne = me.cnt[id + 1] - me.cnt[id];
                    CItem* ck = me.cks.data() + me.ccnt[id];
                    const size_t c1 = me.ccnt[id + 1] - me.ccnt[id];
                    sort_ts(ev, ne, me.tmp.data(), top_hi);
                    if (c1 > 1) sort_clicks(ck, c1);

                    size_t lo = 0, hi = 0;
                    // With many clicks, rescanning the window for every session costs sessions x
                    // clicks: keep per-campaign counts as the window slides instead.
                    const bool sliding = c1 > 48;
                    int cur_day = -1;
                    u32 d_sessions = 0, d_events = 0;
                    u64 d_rev = 0;
                    auto flush = [&]() {
                        if (cur_day < 0) return;
                        int best = -1;
                        for (u32 c : me.touched) {
                            if (best < 0 || me.catsum[c] > me.catsum[best] ||
                                (me.catsum[c] == me.catsum[best] && cat_rank[c] < cat_rank[best]))
                                best = (int)c;
                        }
                        for (u32 c : me.touched) me.catsum[c] = 0;
                        me.touched.clear();
                        out.add(person, (int32_t)(DAY_BASE + cur_day), d_sessions, d_events, d_rev, best);
                        nrows++;
                        d_sessions = d_events = 0;
                        d_rev = 0;
                    };
                    size_t k = 0;
                    while (k < ne) {
                        const u32 start = (u32)(ev[k] >> 32);
                        const int day = (int)(((u64)start + (u64)DAY_OFF) / (u64)DAY_MS);
                        if (day != cur_day) { flush(); cur_day = day; }
                        u32 prev = start, nev = 0;
                        u64 rev = 0;
                        while (k < ne) {
                            const u32 ts = (u32)(ev[k] >> 32);
                            if (ts - prev > GAP_MS) break;
                            prev = ts;
                            nev++;
                            const u32 pc = (u32)ev[k];
                            if (pc) {
                                const u32 price = pc >> 8, cat = pc & 255;
                                rev += price;
                                if (!me.catsum[cat]) me.touched.push_back(cat);
                                me.catsum[cat] += price;
                            }
                            k++;
                        }
                        // credit and assists
                        const u32 low = start >= (u32)WEEK_MS ? start - (u32)WEEK_MS : 0;
                        u32 camp = (u32)NCAMP;
                        if (!sliding) {
                            while (hi < c1 && (u32)(ck[hi].tc >> 32) <= start) hi++;
                            while (lo < hi && (u32)(ck[lo].tc >> 32) < low) lo++;
                            if (hi > lo) {
                                const u32 stamp = ++me.stamp;
                                for (size_t x = lo; x < hi; x++) {
                                    const u32 c = (u32)ck[x].tc;
                                    if (me.seen[c] != stamp) {
                                        me.seen[c] = stamp;
                                        me.hot[(size_t)c * nd + day].assisted++;
                                    }
                                }
                            }
                        } else {
                            for (; hi < c1 && (u32)(ck[hi].tc >> 32) <= start; hi++) {
                                const u32 c = (u32)ck[hi].tc;
                                if (me.wcnt[c]++ == 0) { me.wpos[c] = (u32)me.active.size(); me.active.push_back(c); }
                            }
                            for (; lo < hi && (u32)(ck[lo].tc >> 32) < low; lo++) {
                                const u32 c = (u32)ck[lo].tc;
                                if (--me.wcnt[c] == 0) {
                                    const u32 last = me.active.back();
                                    me.active[me.wpos[c]] = last;
                                    me.wpos[last] = me.wpos[c];
                                    me.active.pop_back();
                                }
                            }
                            for (u32 c : me.active) me.hot[(size_t)c * nd + day].assisted++;
                        }
                        if (hi > lo) {  // the latest click; among those at its ts, the smallest click_id
                            size_t f = hi - 1;
                            const u32 lt = (u32)(ck[f].tc >> 32);
                            while (f > lo && (u32)(ck[f - 1].tc >> 32) == lt) f--;
                            camp = (u32)ck[f].tc;
                        }
                        const size_t cell = (size_t)camp * nd + day;
                        me.hot[cell].sessions++;
                        if (rev) { me.cold[cell].purchasing++; me.cold[cell].revenue += rev; }
                        d_sessions++;
                        d_events += nev;
                        d_rev += rev;
                    }
                    flush();
                    if (sliding) {
                        for (u32 c : me.active) me.wcnt[c] = 0;
                        me.active.clear();
                    }
                }
            }
            out.finish(t == 0);  // thread 0 always writes, so the table always has a file
            total_rows += nrows;
        });
        pool.run([&](int) { unmap_some(ck_files, unmap_ck); });
    }
    lap("sessions built and user_daily written");

    {
        vector<AggHot> hot(ncell, AggHot{0, 0});
        vector<AggCold> cold(ncell, AggCold{0, 0});
        for (auto& c : ctx)
            for (size_t i = 0; i < ncell; i++) {
                hot[i].sessions += c.hot[i].sessions;
                hot[i].assisted += c.hot[i].assisted;
                cold[i].purchasing += c.cold[i].purchasing;
                cold[i].revenue += c.cold[i].revenue;
            }
        PqFile pq;
        pq.create(cd_dir + "/part-000.parquet",
                  {{"campaign_id", C_STR}, {"channel", C_STR}, {"day", C_DATE}, {"sessions", C_I64},
                   {"purchasing_sessions", C_I64}, {"revenue_cents", C_I64}, {"assisted_sessions", C_I64}});
        string pg[7];
        u64 rows = 0;
        for (int c = 0; c <= NCAMP; c++)
            for (int d = 0; d < NDAYS; d++) {
                const size_t i = (size_t)c * NDAYS + d;
                if (!hot[i].sessions && !hot[i].assisted) continue;
                rows++;
                put_str(pg[0], camp_ids[c]);
                put_str(pg[1], camp_channels[c]);
                put32(pg[2], (u32)(int32_t)(DAY_BASE + d));
                put64(pg[3], hot[i].sessions);
                put64(pg[4], cold[i].purchasing);
                put64(pg[5], cold[i].revenue);
                put64(pg[6], hot[i].assisted);
            }
        if (rows) pq.group(rows, pg);
        pq.finish();
        fprintf(stderr, "user_daily rows %llu, campaign_daily rows %llu\n", (unsigned long long)total_rows.load(),
                (unsigned long long)rows);
    }
    lap("campaign_daily written");
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    fprintf(stderr, "threads %d, partitions %d/%u, peak rss %.0f MB\n", NT, P, TP, ru.ru_maxrss / 1024.0);
    // Skip destructors: the process is done.
    fflush(stderr);
    _exit(0);
}
