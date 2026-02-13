// Minimal test harness for header-only orderbook tests
#pragma once

#include <functional>
#include <iostream>
#include <string>
#include <vector>

namespace mini_test {

struct TestCase {
    const char* name;
    std::function<void()> fn;
};

inline std::vector<TestCase>& registry() {
    static std::vector<TestCase> r;
    return r;
}

struct Registrar {
    Registrar(const char* n, std::function<void()> f) { registry().push_back({n, std::move(f)}); }
};

// Simple expectation counters per test
struct Context {
    int failures = 0;
};

inline thread_local Context ctx;

// Expect macros
#define EXPECT_TRUE(cond)                                                                         \
    do {                                                                                          \
        if (!(cond)) {                                                                            \
            ::mini_test::ctx.failures++;                                                          \
            std::cerr << "EXPECT_TRUE failed at " << __FILE__ << ':' << __LINE__ << "\n";       \
        }                                                                                         \
    } while (0)

#define EXPECT_EQ(a, b)                                                                           \
    do {                                                                                          \
        if (!((a) == (b))) {                                                                      \
            ::mini_test::ctx.failures++;                                                          \
            std::cerr << "EXPECT_EQ failed at " << __FILE__ << ':' << __LINE__ << "\n";         \
        }                                                                                         \
    } while (0)

#define TEST(name)                                                                                \
    static void name();                                                                           \
    static ::mini_test::Registrar name##_registrar{#name, &name};                                 \
    static void name()

inline int run_all() {
    int total = 0, failed = 0;
    for (auto& t : registry()) {
        ctx.failures = 0;
        try {
            t.fn();
        } catch (const std::exception& e) {
            ctx.failures++;
            std::cerr << "Unhandled exception in test '" << t.name << "': " << e.what() << "\n";
        } catch (...) {
            ctx.failures++;
            std::cerr << "Unhandled non-std exception in test '" << t.name << "'\n";
        }
        total++;
        if (ctx.failures) {
            failed++;
            std::cout << "[ FAIL ] " << t.name << " (" << ctx.failures << " failures)\n";
        } else {
            std::cout << "[  OK  ] " << t.name << "\n";
        }
    }
    std::cout << "\nSummary: " << (total - failed) << "/" << total << " tests passed\n";
    return failed == 0 ? 0 : 1;
}

} // namespace mini_test
