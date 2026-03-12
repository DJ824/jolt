#include "include/SharedMemoryRing.h"
#include "include/thread_affinity.h"

#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <immintrin.h>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <sys/wait.h>
#include <unistd.h>

namespace {
    constexpr size_t QUEUE_SIZE = 1 << 20;
    constexpr size_t DEFAULT_ITERATIONS = 5'000'000;
    constexpr int DEFAULT_RUNS = 5;

    void pause_cpu() noexcept {
#if defined(__x86_64__) || defined(__i386__)
        _mm_pause();
#else
        std::this_thread::yield();
#endif
    }

    void pinThread(const int cpu_id) {
        if (cpu_id >= 0) {
            (void)jolt::threading::pin_current_thread_to_cpu(cpu_id, "shared-mem-q-bench");
        }
    }

    bool parse_u64(std::string_view s, uint64_t& out) {
        if (s.empty()) {
            return false;
        }
        const char* first = s.data();
        const char* last = s.data() + s.size();
        const auto [ptr, ec] = std::from_chars(first, last, out);
        return ec == std::errc{} && ptr == last;
    }

    template <size_t Bytes>
    struct Payload {
        static_assert(Bytes >= sizeof(uint64_t), "payload must fit sequence field");

        uint64_t value{0};
        std::array<std::byte, Bytes - sizeof(uint64_t)> pad{};

        void set_data(const int v) noexcept { value = static_cast<uint64_t>(v); }
        [[nodiscard]] int seq() const noexcept { return static_cast<int>(value); }
    };

    template <typename Tag>
    void run_payload_bench(const Tag& tag,
                           const std::string& label,
                           const int runs,
                           const size_t num_iterations,
                           const int producer_cpu,
                           const int consumer_cpu) {
        (void)tag;

        std::cout << "[bench] payload=" << label
                  << " queue_slots=" << QUEUE_SIZE
                  << " iterations=" << num_iterations
                  << " runs=" << runs << std::endl;

        SharedRingOptions options{};
        options.unlink_on_destroy = true;
        options.wait_ms = 5000;

        auto run_throughput = [&](auto bench_tag, int run) {
            using P = decltype(bench_tag);
            using QueueT = SharedSpscQueue<P, QUEUE_SIZE>;
            const std::string ring_name =
                "/jolt_shared_spsc_bench_" + std::to_string(::getpid()) + "_" + label + "_" + std::to_string(run);

            auto queue = std::make_unique<QueueT>(ring_name, SharedRingMode::Create, options);

            int ready_pipe[2]{-1, -1};
            if (::pipe(ready_pipe) != 0) {
                std::cerr << "error: pipe() failed: " << std::strerror(errno) << "\n";
                std::exit(1);
            }

            const pid_t child = ::fork();
            if (child < 0) {
                std::cerr << "error: fork() failed: " << std::strerror(errno) << "\n";
                std::exit(1);
            }

            if (child == 0) {
                ::close(ready_pipe[0]);

                if (consumer_cpu >= 0) {
                    pinThread(consumer_cpu);
                }

                try {
                    QueueT consumer_q(ring_name, SharedRingMode::Attach, options);
                    const uint8_t ready = 1;
                    if (::write(ready_pipe[1], &ready, sizeof(ready)) != static_cast<ssize_t>(sizeof(ready))) {
                        std::cerr << "error: child failed to signal readiness\n";
                        _exit(3);
                    }
                    ::close(ready_pipe[1]);

                    int expected_value = 0;
                    for (size_t i = 0; i < num_iterations; ++i) {
                        P result{};
                        while (!consumer_q.try_dequeue(result)) {
                            pause_cpu();
                        }
                        if (result.seq() != expected_value) {
                            std::cerr << "error, expected " << expected_value << " but got "
                                      << result.seq() << std::endl;
                            _exit(1);
                        }
                        expected_value++;
                    }
                    _exit(0);
                } catch (const std::exception& ex) {
                    std::cerr << "error: child attach/consume failed: " << ex.what() << "\n";
                    _exit(2);
                }
            }

            ::close(ready_pipe[1]);
            uint8_t ready = 0;
            const ssize_t nread = ::read(ready_pipe[0], &ready, sizeof(ready));
            ::close(ready_pipe[0]);
            if (nread != static_cast<ssize_t>(sizeof(ready)) || ready != 1) {
                int status = 0;
                (void)::waitpid(child, &status, 0);
                std::cerr << "error: consumer process failed to initialize\n";
                std::exit(1);
            }

            if (producer_cpu >= 0) {
                pinThread(producer_cpu);
            }

            const auto start_time = std::chrono::high_resolution_clock::now();
            for (size_t i = 0; i < num_iterations; ++i) {
                P msg{};
                msg.set_data(static_cast<int>(i));
                while (!queue->enqueue(msg)) {
                    pause_cpu();
                }
            }

            int status = 0;
            if (::waitpid(child, &status, 0) < 0) {
                std::cerr << "error: waitpid() failed: " << std::strerror(errno) << "\n";
                std::exit(1);
            }
            if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
                std::cerr << "error: consumer process exited with status " << WEXITSTATUS(status) << "\n";
                std::exit(1);
            }

            const auto end_time = std::chrono::high_resolution_clock::now();
            const auto duration_ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

            const double throughput_ops_per_ms = (num_iterations * 1'000'000.0) / static_cast<double>(duration_ns);
            const double latency_ns_per_op = static_cast<double>(duration_ns) / static_cast<double>(num_iterations);

            std::cout << "    Run " << (run + 1) << ": "
                      << std::fixed << std::setprecision(2)
                      << throughput_ops_per_ms << " ops/ms, "
                      << latency_ns_per_op << " ns/op" << std::endl;
        };

        for (int run = 0; run < runs; ++run) {
            run_throughput(Tag{}, run);
        }
    }
}

int main(int argc, char** argv) {
    int runs = DEFAULT_RUNS;
    size_t num_iterations = DEFAULT_ITERATIONS;
    int producer_cpu = -1;
    int consumer_cpu = -1;

    for (int i = 1; i < argc; ++i) {
        const std::string_view arg(argv[i]);
        auto require_next = [&](const char* flag) -> std::string_view {
            if (i + 1 >= argc) {
                std::cerr << "missing value for " << flag << "\n";
                std::exit(2);
            }
            return std::string_view(argv[++i]);
        };

        if (arg == "--runs") {
            uint64_t v = 0;
            if (!parse_u64(require_next("--runs"), v) || v == 0 || v > 1000) {
                std::cerr << "invalid --runs\n";
                return 2;
            }
            runs = static_cast<int>(v);
        } else if (arg == "--iterations") {
            uint64_t v = 0;
            if (!parse_u64(require_next("--iterations"), v) || v == 0 || v > static_cast<uint64_t>(INT32_MAX)) {
                std::cerr << "invalid --iterations (must be 1.." << INT32_MAX << ")\n";
                return 2;
            }
            num_iterations = static_cast<size_t>(v);
        } else if (arg == "--producer-cpu") {
            uint64_t v = 0;
            if (!parse_u64(require_next("--producer-cpu"), v) || v > static_cast<uint64_t>(INT32_MAX)) {
                std::cerr << "invalid --producer-cpu\n";
                return 2;
            }
            producer_cpu = static_cast<int>(v);
        } else if (arg == "--consumer-cpu") {
            uint64_t v = 0;
            if (!parse_u64(require_next("--consumer-cpu"), v) || v > static_cast<uint64_t>(INT32_MAX)) {
                std::cerr << "invalid --consumer-cpu\n";
                return 2;
            }
            consumer_cpu = static_cast<int>(v);
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: SharedMemRingBench [--runs N] [--iterations N] "
                      << "[--producer-cpu N] [--consumer-cpu N]\n";
            return 0;
        } else {
            std::cerr << "unknown argument: " << arg << "\n";
            return 2;
        }
    }

    run_payload_bench(Payload<16>{}, "p16", runs, num_iterations, producer_cpu, consumer_cpu);
    run_payload_bench(Payload<64>{}, "p64", runs, num_iterations, producer_cpu, consumer_cpu);
    run_payload_bench(Payload<256>{}, "p256", runs, num_iterations, producer_cpu, consumer_cpu);
    return 0;
}
